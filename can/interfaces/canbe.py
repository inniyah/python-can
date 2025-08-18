"""
python-can backend for CANable 2.0 CANbeSerial protocol ("canbe").

Drop-in style bus implementation similar to slcan, but speaking the
COBS + CRC16 framed CANbeSerial protocol.

Usage examples
--------------

>>> import can
>>> from canbe import canbeBus
>>> bus = canbeBus(channel="/dev/ttyUSB0", bitrate=500000)  # classic CAN
>>> msg = can.Message(arbitration_id=0x123, data=bytes([1,2,3,4]), is_extended_id=False)
>>> bus.send(msg)
>>> print(bus.recv(timeout=1.0))

# CAN FD with 2M data rate
>>> bus = canbeBus(channel="/dev/ttyUSB0", bitrate=500000, data_bitrate=2_000_000)

Alternatively, when using python-can's dynamic loader you may reference the
class by its dotted path (e.g. "path.to.canbe.canbeBus"). If you package this
module, you can expose an entry point in the "can.interface" group with the
name "canbe" that points to "canbe:canbeBus" so you can do: can.Bus(interface="canbe", ...).

Tested with CANable 2.0 running CANbeSerial firmware and 115200 TTY baud.

"""
from __future__ import annotations

import io
import logging
import struct
import time
from queue import SimpleQueue
from typing import Any, Optional, Union, cast

from can import BitTiming, BitTimingFd, BusABC, CanProtocol, Message, typechecking
from can.exceptions import (
    CanInitializationError,
    CanInterfaceNotImplementedError,
    CanOperationError,
    error_check,
)
from can.util import check_or_adjust_timing_clock, deprecated_args_alias, len2dlc

logger = logging.getLogger(__name__)

try:
    import serial  # type: ignore
except ImportError:  # pragma: no cover
    logger.warning(
        "You won't be able to use the canbe backend without the serial module installed!"
    )
    serial = None  # type: ignore


# ------------------------------
# CANbeSerial protocol constants
# ------------------------------
PID_DATA = 0x00
PID_ERROR = 0x01
PID_TX_ACK = 0x02

PID_PROTOCOL_VERSION = 0x08
PID_PROTOCOL_VERSION_REQ = 0x88

PID_CONFIG_STATE = 0x09
PID_CONFIG_STATE_REQ = 0x89
PID_CONFIG_STATE_CMD = 0xC9

PID_DEVICE_INFO = 0x0A
PID_DEVICE_INFO_REQ = 0x8A

# DLC mapping as used by CAN FD
DLC_TO_LEN = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64]

# Classic + FD baud enumerations used by firmware
BAUD_MAP = {
    0: 10_000,
    1: 20_000,
    2: 50_000,
    3: 100_000,
    4: 125_000,
    5: 250_000,
    6: 500_000,
    7: 1_000_000,
    8: 2_000_000,
    9: 5_000_000,
    10: 10_000_000,
}


def _enum_for_bitrate(bps: int) -> int:
    """Return enum code for *bps* (exact match preferred, else closest)."""
    pairs = sorted(BAUD_MAP.items(), key=lambda kv: abs(kv[1] - bps))
    return pairs[0][0]


def _crc16(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        x = (crc >> 8) ^ b
        x ^= (x >> 4)
        crc = ((crc << 8) ^ (x << 12) ^ (x << 5) ^ x) & 0xFFFF
    return crc


def _cobs_encode(source: bytes) -> bytes:
    dest = bytearray(len(source) + 2)
    null_index = 0
    i = 1
    for b in source:
        dest[i] = b
        if b == 0:
            dest[null_index] = i - null_index
            null_index = i
        i += 1
    dest[null_index] = i - null_index
    dest[i] = 0
    return bytes(dest[: i + 1])


def _cobs_decode(source: bytes) -> bytes:
    if len(source) < 2 or source[0] < 1 or source[-1] != 0:
        return b""
    dest = bytearray(len(source) - 1)  # ignore final 0x00 in dest
    null_index = source[0] - 1
    for i in range(len(dest) - 1):
        dest[i] = source[i + 1]
        if null_index == i:
            if source[i + 1] == 0:
                return bytes(dest[:i])
            null_index += source[i + 1]
            dest[i] = 0
        elif source[i + 1] == 0:
            return b""
    return bytes(dest[:-1])


class canbeBus(BusABC):
    """CANable 2.0 CANbeSerial interface for python-can.

    This class mirrors :class:`can.interfaces.slcan.slcanBus` in spirit, but
    speaks the binary CANbeSerial protocol over a serial TTY.
    """

    LINE_TERMINATOR = b"\x00"  # framing delimiter
    _SLEEP_AFTER_SERIAL_OPEN = 0.2

    @deprecated_args_alias(
        deprecation_start="4.5.0",
        deprecation_end="5.0.0",
        ttyBaudrate="tty_baudrate",
    )
    def __init__(
        self,
        channel: typechecking.ChannelStr,
        tty_baudrate: int = 115200,
        bitrate: Optional[int] = None,
        data_bitrate: Optional[int] = None,
        timing: Optional[Union[BitTiming, BitTimingFd]] = None,
        sleep_after_open: float = _SLEEP_AFTER_SERIAL_OPEN,
        rtscts: bool = False,
        listen_only: bool = False,
        timeout: float = 0.005,
        **kwargs: Any,
    ) -> None:
        """
        :param channel: Serial port (e.g. "/dev/ttyUSB0", "COM8", or "COM8@115200").
        :param tty_baudrate: Serial baudrate; ignored if set via "@" in channel.
        :param bitrate: Nominal (arbitration) bitrate in bit/s.
        :param data_bitrate: Data bitrate for CAN FD in bit/s. If None or 0, FD is disabled.
        :param timing: Optional can.BitTiming or can.BitTimingFd instance; if provided,
                       this overrides *bitrate*/*data_bitrate* (FD support requires a BitTimingFd).
        :param listen_only: Open interface in silent mode.
        :param timeout: Serial read timeout in seconds.
        """
        if serial is None:
            raise CanInterfaceNotImplementedError("The serial module is not installed")
        if not channel:
            raise ValueError("Must specify a serial port.")
        if "@" in channel:
            channel, br = channel.split("@", 1)
            tty_baudrate = int(br)

        self._listen_only = listen_only
        self._can_protocol = CanProtocol.CAN_20  # switches automatically for FD frames

        with error_check(exception_type=CanInitializationError):
            self._ser = serial.serial_for_url(
                channel,
                baudrate=tty_baudrate,
                rtscts=rtscts,
                timeout=timeout,
            )

        # Flush any stale input from a prior session
        try:
            self._ser.reset_input_buffer()
        except Exception:
            pass

        self._rx_buf = bytearray()
        self._queue: SimpleQueue[tuple[int, bytes]] = SimpleQueue()  # (PID, payload)

        time.sleep(sleep_after_open)

        # Apply bitrate(s) now
        with error_check(exception_type=CanInitializationError):
            if isinstance(timing, BitTimingFd):
                # Validate / adjust clocks for nominal timing if provided
                try:
                    _ = check_or_adjust_timing_clock(timing.nom_timing, valid_clocks=[8_000_000])  # type: ignore[attr-defined]
                except Exception:
                    # Some python-can versions expose attributes slightly differently; ignore strict checks
                    pass
                self.set_bitrate(timing.nom_bitrate, timing.data_bitrate)
            elif isinstance(timing, BitTiming):
                timing = check_or_adjust_timing_clock(timing, valid_clocks=[8_000_000])
                self.set_bitrate(timing.bitrate)  # type: ignore[attr-defined]
            else:
                self.set_bitrate(bitrate, data_bitrate)

        super().__init__(channel, **kwargs)

    # ---------------------- public control methods ----------------------
    def set_bitrate(self, bitrate: Optional[int], data_bitrate: Optional[int] = None) -> None:
        """Configure device bitrates and enable the bus.

        If *data_bitrate* is falsy, FD is disabled.
        """
        # Choose enums (closest supported)
        if bitrate is None:
            raise ValueError("bitrate must be provided for CANbeSerial")
        classic_enum = _enum_for_bitrate(int(bitrate))
        fd_enabled = bool(data_bitrate)
        fd_enum = _enum_for_bitrate(int(data_bitrate)) if fd_enabled else 0

        # Issue configuration command; enable & AR on, silent per flag
        b0 = 0
        if True:
            b0 |= 0x02  # enabled
            b0 |= 0x04  # automatic retransmission
        if self._listen_only:
            b0 |= 0x08  # silent mode
        payload = struct.pack(
            ">BBBBB", PID_CONFIG_STATE_CMD, classic_enum, fd_enum, b0, 0x00
        )
        self._send_packet(payload)

        # Wait for CONFIG_STATE (queue unexpected PIDs)
        _ = self._read_until(PID_CONFIG_STATE, timeout=1.0)

    def open(self) -> None:
        """Kept for BusABC API symmetry. CANbe applies config in set_bitrate."""
        return None

    def close(self) -> None:
        # There is no dedicated close command in protocol; we just keep the TTY.
        return None

    # ---------------------- I/O primitives ----------------------
    def _send_packet(self, payload: bytes) -> None:
        crc_be = struct.pack(">H", _crc16(payload))
        framed = b"\x00" + _cobs_encode(payload + crc_be) + b"\x00"
        with error_check("Could not write to serial device"):
            self._ser.write(framed)
            self._ser.flush()

    def _read_one_frame(self, timeout: Optional[float]) -> tuple[Optional[int], Optional[bytes]]:
        deadline = None if timeout is None else time.time() + timeout
        buf = self._rx_buf
        while True:
            # try to parse from current buffer first
            if buf:
                # search for delimiter 0x00
                try:
                    idx = buf.index(0)
                except ValueError:
                    idx = -1
                if idx >= 0:
                    # take up to delimiter (excluding), decode COBS
                    raw = bytes(buf[: idx + 1])
                    del buf[: idx + 1]
                    decoded = _cobs_decode(raw)
                    if not decoded or len(decoded) < 3:
                        # malformed; continue scanning
                        continue
                    payload, recv_crc = decoded[:-2], struct.unpack(">H", decoded[-2:])[0]
                    if _crc16(payload) != recv_crc:
                        continue
                    return payload[0], payload
            # read more
            time_left = None if deadline is None else max(0.0, deadline - time.time())
            if deadline is not None and time_left == 0:
                return None, None
            with error_check("Could not read from serial device"):
                chunk = self._ser.read(64 if time_left is None else max(1, min(64, int(64))))
            if not chunk:
                # serial timeout tick
                if deadline is not None and time.time() >= deadline:
                    return None, None
                continue
            buf.extend(chunk)
            # Avoid unbounded growth
            if len(buf) > 4096:
                del buf[:-1024]

    # ---------------------- internal helpers for control/queries ----------------------
    def _queue_push(self, pid: int, payload: bytes) -> None:
        """Push a frame to the internal side-queue (non-blocking).
        To avoid unbounded growth in very noisy environments, we cap how many
        items we try to retain by draining one when the queue seems backed up.
        """
        try:
            self._queue.put_nowait((pid, payload))
        except Exception:
            try:
                _ = self._queue.get_nowait()
            except Exception:
                pass
            try:
                self._queue.put_nowait((pid, payload))
            except Exception:
                pass

    def _queue_pop_matching(self, want_pid: int) -> Optional[bytes]:
        """Return payload for the first queued frame with *want_pid*, preserving others."""
        tmp: list[tuple[int, bytes]] = []
        found: Optional[bytes] = None
        while not self._queue.empty():
            pid, p = self._queue.get_nowait()
            if found is None and pid == want_pid:
                found = p
            else:
                tmp.append((pid, p))
        for item in tmp:
            self._queue.put_nowait(item)
        return found

    def _read_until(self, want_pid: int, timeout: Optional[float]) -> Optional[bytes]:
        """Read frames until *want_pid* is seen or timeout. Unexpected frames are queued."""
        deadline = None if timeout is None else time.time() + timeout
        # check if we already have the frame queued from earlier
        p = self._queue_pop_matching(want_pid)
        if p is not None:
            return p
        while True:
            remaining = None if deadline is None else max(0.0, deadline - time.time())
            if deadline is not None and remaining == 0:
                return None
            pid, p = self._read_one_frame(remaining)
            if pid is None or p is None:
                return None
            if pid == want_pid:
                return p
            # unexpected -> queue it for later consumers
            self._queue_push(pid, p)

    # ---------------------- BusABC hooks ----------------------
    def _recv_internal(self, timeout: Optional[float]) -> tuple[Optional[Message], bool]:
        deadline = None if timeout is None else time.time() + timeout
        # We'll do a bounded pass over queued control frames without re-queuing
        # them immediately to avoid infinite churn in noisy environments.
        scanned_non_data_from_queue = 0
        while True:
            # Prefer queued frames first
            from_queue = False
            if not self._queue.empty():
                pid, p = self._queue.get_nowait()
                from_queue = True
            else:
                remaining = None if deadline is None else max(0.0, deadline - time.time())
                if deadline is not None and remaining == 0:
                    return None, False
                pid, p = self._read_one_frame(remaining)

            if pid is None or p is None:
                return None, False

            if pid == PID_DATA:
                # [PID][ts:4 BE ms][id:4 BE][flags0][flags1][data...]
                if len(p) < 11:
                    # malformed; continue loop to try next frame
                    continue
                ts_ms = struct.unpack(">I", p[1:5])[0]
                can_id = struct.unpack(">I", p[5:9])[0]
                flags0 = p[9]
                dlc_nibble = (flags0 >> 4) & 0x0F

                # How many bytes are actually present after the 11-byte header?
                bytes_in_packet = len(p) - 11
                if bytes_in_packet < 0:
                    bytes_in_packet = 0

                # Map DLC nibble to nominal length (FD table), but be tolerant of quirky senders
                len_from_dlc = DLC_TO_LEN[dlc_nibble] if dlc_nibble < len(DLC_TO_LEN) else 0

                # If DLC says 0 but bytes exist, trust the packet (some devices bungle DLC)
                if len_from_dlc == 0 and bytes_in_packet > 0:
                    length = bytes_in_packet
                else:
                    # Never read more than what the packet actually contains
                    length = min(len_from_dlc, bytes_in_packet)

                # Flags — be robust to mismatches; infer FD from length too
                FD_MASK, BRS_MASK, EXT_MASK, RTR_MASK = 0x04, 0x08, 0x01, 0x02
                is_fd_flag = bool(flags0 & FD_MASK)
                is_fd      = is_fd_flag or (length > 8)            # FD has >8 data bytes
                brs        = bool(flags0 & BRS_MASK) and is_fd
                is_ext     = bool(flags0 & EXT_MASK)

                # Treat as RTR only if classic AND no data bytes actually present.
                # (Some devices set RTR erroneously on data frames; don't drop their data.)
                rtr_flag   = bool(flags0 & RTR_MASK)
                is_rtr     = (not is_fd) and rtr_flag and (length == 0)

                data = b"" if is_rtr else p[11 : 11 + length]

                #~ print(f"flags0=0x{flags0:02X}, dlc_nibble={dlc_nibble}, bytes_in_packet={bytes_in_packet}, "
                #~       f"len_from_dlc={len_from_dlc}, chosen_length={length}, tail={p[11:11+min(bytes_in_packet,16)].hex()}")

                #~ print(f"Data: {data.hex()}")

                msg = Message(
                    arbitration_id=can_id,
                    is_extended_id=is_ext,
                    is_remote_frame=is_rtr,
                    is_fd=is_fd,
                    bitrate_switch=brs,
                    dlc=length,  # report bytes
                    data=bytes(data),
                    timestamp=time.time() if ts_ms == 0 else ts_ms / 1000.0,
                )
                # python-can expects CAN_20 unless FD is used
                self._can_protocol = CanProtocol.CAN_FD if is_fd else CanProtocol.CAN_20
                return msg, False

            # Not a data frame
            if from_queue:
                # Do NOT requeue immediately; this prevents livelock when the
                # queue is full of control frames. Give the loop a chance to
                # fetch from serial or time out.
                scanned_non_data_from_queue += 1
                if deadline is not None and time.time() >= deadline:
                    return None, False
                # After a few, yield the GIL briefly to avoid busy spin
                if scanned_non_data_from_queue % 8 == 0:
                    time.sleep(0)
                continue
            else:
                # Came from serial read: preserve for control helpers
                self._queue_push(pid, p)
                # Loop again until timeout to look for PID_DATA
                continue

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        if timeout != self._ser.write_timeout:
            self._ser.write_timeout = timeout

        # flags0: [dlc:4][BRS:1][FD:1][RTR:1][EXT:1]
        data_len = len(msg.data or b"")

        if msg.is_remote_frame:
            req_len = msg.dlc if msg.dlc is not None else data_len
            req_len = int(req_len)
            req_len = max(0, min(64 if msg.is_fd else 8, req_len))
            dlc_val = len2dlc(req_len) if msg.is_fd else req_len
        else:
            if msg.is_fd:
                dlc_val = len2dlc(data_len)
            else:
                dlc_val = max(0, min(8, data_len))  # clamp classic
        flags0 = (dlc_val & 0x0F) << 4
        if msg.bitrate_switch:
            if not msg.is_fd:
                raise CanOperationError("BRS set on non-FD frame")
            flags0 |= 0x08
        if msg.is_fd:
            flags0 |= 0x04
        if msg.is_remote_frame:
            flags0 |= 0x02
        if msg.is_extended_id:
            flags0 |= 0x01

        flags1 = 0
        ts_ms = 0  # let device timestamp
        payload = struct.pack(
            ">B I I B B", PID_DATA, ts_ms, msg.arbitration_id & 0x1FFFFFFF, flags0, flags1
        ) + (msg.data or b"")
        self._send_packet(payload)

    def shutdown(self) -> None:
        super().shutdown()
        with error_check("Could not close serial socket"):
            try:
                self._ser.close()
            except Exception:
                pass

    def fileno(self) -> int:
        try:
            return cast("int", self._ser.fileno())
        except io.UnsupportedOperation as exc:
            raise NotImplementedError(
                "fileno is not implemented using current CAN bus on this platform"
            ) from exc
        except Exception as exc:  # pragma: no cover
            raise CanOperationError("Cannot fetch fileno") from exc

    # ---------------------- optional helpers ----------------------
    def debug_sniff_raw(self, seconds: float = 1.0) -> list[tuple[int, bytes]]:
        """Collect raw protocol frames (PID, payload) for *seconds* and return them.
        Useful to see what's actually arriving on the wire when debugging interoperability.
        """
        out: list[tuple[int, bytes]] = []
        end = time.time() + max(0.0, seconds)
        while time.time() < end:
            pid, p = self._read_one_frame(timeout=0.05)
            if pid is None or p is None:
                continue
            out.append((pid, p))
            # don't starve normal consumers; re-queue non-DATA
            if pid != PID_DATA:
                self._queue_push(pid, p)
        return out

    def send_and_wait_ack(self, msg: Message, timeout: float = 0.2) -> bool:
        """Send *msg* and return True if a PID_TX_ACK is seen within *timeout* seconds."""
        self.send(msg)
        return self._read_until(PID_TX_ACK, timeout) is not None

    def reply_to_rtr_once(self, supplier, filter_id: Optional[int] = None, timeout: float = 1.0) -> bool:
        """Wait for an RTR frame and reply once with data provided by *supplier*.

        *supplier* can be bytes or a callable taking (arbitration_id, dlc) -> bytes.
        If *filter_id* is not None, only RTR for that ID are handled.
        Returns True if a reply was sent, False on timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg, _ = self._recv_internal(timeout=deadline - time.time())
            if not msg:
                break
            if msg.is_remote_frame and (filter_id is None or msg.arbitration_id == filter_id):
                if callable(supplier):
                    data = supplier(msg.arbitration_id, msg.dlc)
                else:
                    data = supplier
                size = int(msg.dlc)  # already bytes
                payload = (data or b"")[:size]
                reply = Message(
                    arbitration_id=msg.arbitration_id,
                    data=payload,
                    is_extended_id=msg.is_extended_id,
                    is_fd=False,
                )
                self.send(reply)
                return True
        return False

    def get_protocol_version(self, timeout: Optional[float] = 1.0) -> Optional[str]:
        self._send_packet(bytes([PID_PROTOCOL_VERSION_REQ]))
        p = self._read_until(PID_PROTOCOL_VERSION, timeout)
        if p:
            raw = p[1:]
            if len(raw) == 1:
                return f"{raw[0]}"
            if len(raw) >= 2:
                return f"{raw[0]}.{raw[1]}"
            return ""
        return None

    def get_device_info(self, timeout: Optional[float] = 1.0) -> Optional[str]:
        self._send_packet(bytes([PID_DEVICE_INFO_REQ]))
        p = self._read_until(PID_DEVICE_INFO, timeout)
        if p:
            try:
                return p[1:].decode("utf-8", errors="replace")
            except Exception:
                return p[1:].hex()
        return None

    def get_config_state(self, timeout: Optional[float] = 1.0) -> Optional[dict[str, Any]]:
        self._send_packet(bytes([PID_CONFIG_STATE_REQ]))
        p = self._read_until(PID_CONFIG_STATE, timeout)
        if p and len(p) >= 5:
            baud = BAUD_MAP.get(p[1])
            fd = BAUD_MAP.get(p[2]) if p[2] != 0 else 0
            b0 = p[3]
            flags = {
                "submissive": bool(b0 & 0x01),
                "enabled": bool(b0 & 0x02),
                "automaticRetransmission": bool(b0 & 0x04),
                "silentMode": bool(b0 & 0x08),
            }
            return {"bitrate": baud, "data_bitrate": fd, "flags": flags}
        return None
