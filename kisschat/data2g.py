"""Client for Data2G's command, broadcast KISS, and connected ARQ interfaces."""
from __future__ import annotations

from collections import deque
import queue
import socket
import threading
import time
from .ax25 import (decode_ui_frame, encode_ui_frame, pack_batch,
                   unpack_batch, ui_reception)
from .protocol import unpack

FEND, FESC, TFEND, TFESC = 0xC0, 0xDB, 0xDC, 0xDD
KISS_DATA, KISS_ACKMODE = 0x00, 0x0C
KISS_ACK_WINDOW = 32
KISS_ACK_TIMEOUT = 180.0


def encode_kiss(port: int, command: int, payload: bytes) -> bytes:
    body = bytes(((port << 4) | command,)) + payload
    output = bytearray((FEND,))
    for value in body:
        if value == FEND:
            output.extend((FESC, TFEND))
        elif value == FESC:
            output.extend((FESC, TFESC))
        else:
            output.append(value)
    output.append(FEND)
    return bytes(output)


class KissDecoder:
    def __init__(self):
        self.frame = bytearray()
        self.inside = self.escaped = False

    def feed(self, data: bytes):
        for value in data:
            if value == FEND:
                if self.inside and self.frame:
                    header = self.frame[0]
                    yield header >> 4, header & 15, bytes(self.frame[1:])
                self.frame.clear()
                self.inside, self.escaped = True, False
            elif not self.inside:
                continue
            elif self.escaped:
                if value == TFEND:
                    self.frame.append(FEND)
                elif value == TFESC:
                    self.frame.append(FESC)
                else:
                    self.frame.clear()
                    self.inside = False
                self.escaped = False
            elif value == FESC:
                self.escaped = True
            else:
                self.frame.append(value)
                if len(self.frame) > 65536:
                    self.frame.clear()
                    self.inside = False


class Data2G:
    """Open a radio transport and deliver application records to the chat."""

    def __init__(self, host: str, callsign: str, *, command_port: int = 8300,
                 kiss_port: int = 8100, mode: str = "", transport: str = "broadcast",
                 bandwidth: str = "2300",
                 ax25_destination: str = "CQ", digipeaters: tuple[str, ...] = (),
                 kiss_tnc_port: int = 0,
                 ax25_baud: int = 1200, ax25_interframe_delay: float = 0.25,
                 unattended: bool = False,
                 on_frame=None, on_status=None, on_debug=None, on_frame_context=None):
        self.host, self.callsign = host, callsign.upper()
        if transport not in {"broadcast", "connected", "ax25"}:
            raise ValueError("transport must be broadcast, connected, or ax25")
        if unattended and transport != "broadcast":
            raise ValueError("--unattended currently requires Data2G broadcast transport")
        if bandwidth not in {"500", "1200", "2300", "2750"}:
            raise ValueError("bandwidth must be 500, 1200, 2300, or 2750")
        self.command_port, self.kiss_port, self.mode = command_port, kiss_port, mode
        self.transport = transport
        self.unattended = unattended
        self.ax25_destination = ax25_destination.upper()
        self.digipeaters = tuple(hop.upper() for hop in digipeaters)
        self.kiss_tnc_port = kiss_tnc_port
        if transport == "ax25":
            # Reject invalid RF addressing at startup rather than retrying forever.
            encode_ui_frame(self.callsign, self.ax25_destination, self.digipeaters, b"")
            if type(kiss_tnc_port) is not int or not 0 <= kiss_tnc_port <= 15:
                raise ValueError("KISS TNC port must be between 0 and 15")
            if type(ax25_baud) is not int or not 50 <= ax25_baud <= 1_000_000:
                raise ValueError("AX.25 baud rate must be between 50 and 1000000")
            if (not isinstance(ax25_interframe_delay, (int, float)) or
                    not 0 <= ax25_interframe_delay <= 60):
                raise ValueError("AX.25 interframe delay must be between 0 and 60 seconds")
        self.ax25_baud = ax25_baud
        self.ax25_interframe_delay = float(ax25_interframe_delay)
        self.bandwidth = bandwidth
        self.on_frame, self.on_status = on_frame, on_status
        self.on_frame_context = on_frame_context
        self.on_debug = on_debug
        self.command_socket = self.kiss_socket = self.data_socket = None
        self.port = None
        self.outbound: queue.Queue[tuple[bytes, bool] | None] = queue.Queue(maxsize=128)
        self.bulk_outbound: queue.Queue[tuple[bytes, bool]] = queue.Queue(maxsize=384)
        self.stop_event = threading.Event()
        self.shutdown_event = threading.Event()
        self.connected_event = threading.Event()
        self.session_event = threading.Event()
        self.command_lock = threading.Lock()
        self.live_command_lock = threading.Lock()
        self.live_command_pending = threading.Event()
        self.live_command_responses: queue.Queue[str] = queue.Queue()
        self.auto_mode = self.manual_mode = ""
        self.current_broadcast_mode = ""
        self.mode_catalog: list[tuple[str, float]] = []
        self.unattended_available = False
        self.policy_requests: queue.Queue[tuple[bool, threading.Event, list[Exception]]] = queue.Queue()
        self.threads: list[threading.Thread] = []
        self.supervisor: threading.Thread | None = None
        self.tag = 0
        self.ack_condition = threading.Condition()
        self.pending_acks: dict[bytes, float] = {}
        self.ack_order: deque[bytes] = deque()
        self.tx_state_lock = threading.Lock()
        self.tx_state = "idle"
        self.tx_state_since = time.monotonic()
        self.tx_last_submit = 0.0

    def transmit_snapshot(self) -> dict:
        """Return a small, UI-safe snapshot of the transmit pipeline."""
        with self.tx_state_lock:
            state = self.tx_state
            age = max(0, int(time.monotonic() - self.tx_state_since))
            last_submit = self.tx_last_submit
        with self.ack_condition:
            pending_acks = len(self.pending_acks)
            oldest_ack = max((time.monotonic() - sent_at
                              for sent_at in self.pending_acks.values()), default=0)
        return {
            "queued": self.outbound.qsize() + self.bulk_outbound.qsize(),
            "state": state,
            "age": age,
            "pending_acks": pending_acks,
            "oldest_ack": int(oldest_ack),
            "last_submit": last_submit,
            "transport": self.transport,
        }

    def _set_tx_state(self, state: str) -> None:
        with self.tx_state_lock:
            self.tx_state = state
            self.tx_state_since = time.monotonic()

    @staticmethod
    def _line(sock: socket.socket, timeout: float = 5) -> str:
        sock.settimeout(timeout)
        buffer = bytearray()
        while len(buffer) < 16384:
            part = sock.recv(1)
            if not part:
                raise ConnectionError("Data2G command connection closed")
            if part == b"\r":
                return buffer.decode("ascii", "replace").strip()
            if part != b"\n":
                buffer.extend(part)
        raise ConnectionError("Data2G command response exceeded 16 KiB")

    def _command(self, sock: socket.socket, command: str, terminal: str) -> list[str]:
        with self.command_lock:
            sock.sendall(command.encode("ascii") + b"\r")
        lines = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            line = self._line(sock, max(0.1, deadline - time.monotonic()))
            lines.append(line)
            if line == "WRONG":
                raise RuntimeError(f"Data2G refused {command}")
            if line.startswith(terminal):
                return lines
            if line == "OK" and terminal == "OK":
                return lines
        raise TimeoutError(f"Timed out waiting for Data2G response to {command}")

    def _command_modes(self, sock: socket.socket) -> list[tuple[str, float]]:
        """Read Data2G's mode catalog before the command reader starts."""
        with self.command_lock:
            sock.sendall(b"MODES\r")
        lines = []
        deadline = time.monotonic() + 10
        quiet_until = None
        while time.monotonic() < deadline:
            try:
                line = self._line(sock, min(1.0, max(0.1, deadline - time.monotonic())))
            except (TimeoutError, socket.timeout):
                if lines and quiet_until is not None and time.monotonic() >= quiet_until:
                    break
                continue
            if line == "WRONG":
                raise RuntimeError("Data2G host refused MODES; cannot verify unattended bandwidth")
            if line == "OK":
                break
            if line:
                lines.append(line)
                quiet_until = time.monotonic() + 0.3
        modes = []
        for line in lines:
            fields = line.split()
            if not fields or fields[0] != "MODE":
                continue
            if len(fields) != 7:
                raise RuntimeError(f"malformed Data2G MODES response: {line!r}")
            try:
                modes.append((fields[1], float(fields[2])))
            except ValueError as exc:
                raise RuntimeError(f"malformed Data2G MODES response: {line!r}") from exc
        if not modes:
            raise RuntimeError("Data2G host returned no modes; cannot verify unattended bandwidth")
        return modes

    def _set_live_broadcast_mode(self, mode: str) -> None:
        if not mode or self.transport != "broadcast":
            return
        with self.live_command_lock:
            while True:
                try:
                    self.live_command_responses.get_nowait()
                except queue.Empty:
                    break
            self.live_command_pending.set()
            try:
                with self.command_lock:
                    self.command_socket.sendall(
                        f"BCAST MODE {self.port} {mode}\r".encode("ascii"))
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    try:
                        line = self.live_command_responses.get(
                            timeout=max(0.1, deadline - time.monotonic()))
                    except queue.Empty as exc:
                        raise TimeoutError(f"timed out selecting Data2G mode {mode}") from exc
                    if line == "WRONG":
                        raise RuntimeError(f"Data2G host refused mode {mode}")
                    if line == "OK":
                        if self.on_debug:
                            self.on_debug("BANDWIDTH_MODE", {"mode": mode})
                        return
            finally:
                self.live_command_pending.clear()

    def set_unattended(self, enabled: bool, timeout: float = 20.0) -> None:
        """Switch policy in the TX worker so mode changes cannot race a frame."""
        if self.transport != "broadcast":
            raise RuntimeError("unattended bandwidth control requires Data2G broadcast")
        if not self.connected_event.is_set():
            raise ConnectionError("Data2G broadcast link is not connected")
        if not self.unattended_available:
            raise RuntimeError("Data2G host has no validated mode at or below 500 Hz")
        done = threading.Event()
        errors: list[Exception] = []
        self.policy_requests.put((bool(enabled), done, errors))
        if not done.wait(timeout):
            raise TimeoutError("timed out changing unattended bandwidth policy")
        if errors:
            raise RuntimeError(str(errors[0])) from errors[0]

    def _apply_policy_requests(self) -> None:
        while True:
            try:
                enabled, done, errors = self.policy_requests.get_nowait()
            except queue.Empty:
                return
            try:
                target = self.auto_mode if enabled else self.manual_mode
                if target and target != self.current_broadcast_mode:
                    self._set_live_broadcast_mode(target)
                    self.current_broadcast_mode = target
                self.unattended = enabled
                if self.on_debug:
                    self.on_debug("BANDWIDTH_POLICY_CHANGED", {
                        "unattended": enabled, "mode": target})
            except Exception as exc:
                errors.append(exc)
            finally:
                done.set()

    def start(self) -> None:
        """Start a reconnecting connection manager and return immediately."""
        if self.supervisor and self.supervisor.is_alive():
            return
        self.shutdown_event.clear()
        self.stop_event.set()
        self.supervisor = threading.Thread(target=self._supervise, name="data2g-supervisor",
                                           daemon=True)
        self.supervisor.start()

    def _connect_once(self) -> None:
        call = self.callsign
        if not 1 <= len(call) <= 10 or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/-" for c in call):
            raise ValueError("callsign must be 1–10 characters from A–Z, 0–9, /, -")
        if self.transport == "ax25":
            # Raw KISS has no command channel; the TNC endpoint is the transport.
            self.kiss_socket = socket.create_connection((self.host, self.kiss_port), timeout=5)
            self.port = 0
        else:
            self.command_socket = socket.create_connection((self.host, self.command_port), timeout=5)
            self._command(self.command_socket, "VERSION", "VERSION Data2G ")
        if self.transport == "broadcast":
            try:
                modes = self._command_modes(self.command_socket)
            except (RuntimeError, TimeoutError, OSError):
                if self.unattended:
                    raise RuntimeError("cannot read Data2G MODES catalog for unattended operation")
                modes = []
            self.mode_catalog = modes
            narrow = [mode for mode in modes if mode[1] <= 500]
            self.unattended_available = bool(narrow)
            if self.unattended and not narrow:
                raise RuntimeError("unattended mode requires a Data2G broadcast mode at or below 500 Hz")
            configured = self.mode
            if configured and modes and configured not in {mode[0] for mode in modes}:
                raise RuntimeError(f"Data2G mode {configured!r} is not in the host MODES catalog")
            response = self._command(self.command_socket, f"BCAST OPEN CHAT FROM {call}", "BCAST PORT ")
            self.port = int(next(line.split()[2] for line in response if line.startswith("BCAST PORT ")))
            if modes:
                for candidate, _width in sorted(narrow, key=lambda mode: mode[1]):
                    try:
                        self._command(self.command_socket,
                                      f"BCAST MODE {self.port} {candidate}", "OK")
                    except RuntimeError:
                        continue
                    self.auto_mode = candidate
                    break
                self.unattended_available = bool(self.auto_mode)
                if self.unattended and not self.unattended_available:
                    raise RuntimeError("Data2G refused every broadcast mode at or below 500 Hz")
                manual_candidates = ([next(mode for mode in modes if mode[0] == configured)]
                                     if configured else sorted(modes, key=lambda mode: mode[1], reverse=True))
                for candidate, _width in manual_candidates:
                    try:
                        self._command(self.command_socket,
                                      f"BCAST MODE {self.port} {candidate}", "OK")
                    except RuntimeError:
                        if configured:
                            raise
                        continue
                    self.manual_mode = candidate
                    break
                if not self.manual_mode:
                    if self.auto_mode:
                        self.manual_mode = self.auto_mode
                    if self.unattended or configured:
                        if not self.manual_mode:
                            raise RuntimeError("Data2G refused every advertised operator-send mode")
                selected_mode = self.auto_mode if self.unattended else self.manual_mode
                if selected_mode:
                    self._command(self.command_socket,
                                  f"BCAST MODE {self.port} {selected_mode}", "OK")
            else:
                selected_mode = self.mode
                self.manual_mode = self.mode
                if selected_mode:
                    self._command(self.command_socket, f"BCAST MODE {self.port} {selected_mode}", "OK")
            if selected_mode:
                self.current_broadcast_mode = selected_mode
            if modes and self.on_debug:
                self.on_debug("BANDWIDTH_POLICY", {
                    "automatic_mode": self.auto_mode,
                    "automatic_hz": next((width for name, width in modes
                                           if name == self.auto_mode), None),
                    "operator_mode": self.manual_mode,
                })
            self.kiss_socket = socket.create_connection((self.host, self.kiss_port), timeout=5)
        elif self.transport == "connected":
            self._command(self.command_socket, f"MYCALL {call}", "OK")
            self._command(self.command_socket, f"BW{self.bandwidth}", "OK")
            self._command(self.command_socket, "CHAT ON", "OK")
            self.data_socket = socket.create_connection((self.host, self.command_port + 1), timeout=5)
            self._command(self.command_socket, "LISTEN ON", "OK")
            self.port = 0  # application callback port; ARQ has no KISS port number
        if self.command_socket:
            self.command_socket.settimeout(None)
        if self.kiss_socket:
            self.kiss_socket.settimeout(None)
        if self.data_socket:
            self.data_socket.settimeout(None)
        self.threads = []
        if self.command_socket:
            self.threads.append(threading.Thread(target=self._read_commands,
                                                 name="data2g-status", daemon=True))
        if self.transport in {"broadcast", "ax25"}:
            self.threads += [threading.Thread(target=self._read_kiss,
                                              name="kiss-chat-rx", daemon=True),
                             threading.Thread(target=self._write_kiss,
                                              name="kiss-chat-tx", daemon=True)]
        elif self.transport == "connected":
            self.threads += [threading.Thread(target=self._read_data,
                                              name="arq-chat-rx", daemon=True),
                             threading.Thread(target=self._write_data,
                                              name="arq-chat-tx", daemon=True)]
        for thread in self.threads:
            thread.start()
        self.connected_event.set()
        label = (f"KISS broadcast CHAT port {self.port}" if self.transport == "broadcast"
                 else "AX.25 UI over raw KISS" if self.transport == "ax25"
                 else "VARA-compatible ARQ ready; listening for connected peers")
        self._status(f"Connected to Data2G at {self.host}; {label}" if self.transport != "ax25"
                     else f"Connected to raw KISS TNC at {self.host}:{self.kiss_port}; {label}")
        if self.unattended:
            self._status(f"Unattended bandwidth policy: automatic={self.auto_mode}; "
                         f"operator sends={self.manual_mode}")

    def _transport_label(self) -> str:
        return "KISS" if self.transport == "ax25" else "Data2G"

    def _supervise(self) -> None:
        backoff = 1.0
        while not self.shutdown_event.is_set():
            self.stop_event.clear()
            try:
                self._connect_once()
                connected_at = time.monotonic()
                while not self.shutdown_event.is_set() and not self.stop_event.wait(0.2):
                    pass
                if time.monotonic() - connected_at >= 30:
                    backoff = 1.0
            except Exception as exc:
                if not self.shutdown_event.is_set():
                    self._status(f"{self._transport_label()} connection failed: {exc}")
            finally:
                self.connected_event.clear()
                self.stop_event.set()
                self._close_sockets()
                for thread in self.threads:
                    if thread is not threading.current_thread():
                        thread.join(timeout=1)
                self.threads = []
            if self.shutdown_event.is_set():
                break
            self._status(f"{self._transport_label()} disconnected; retrying in {backoff:.0f}s")
            self.shutdown_event.wait(backoff)
            backoff = min(backoff * 2, 30.0)

    def _close_sockets(self) -> None:
        self.session_event.clear()
        with self.ack_condition:
            abandoned = len(self.pending_acks)
            self.pending_acks.clear()
            self.ack_order.clear()
            self.ack_condition.notify_all()
        if abandoned:
            self._status(f"{abandoned} KISS ACKMODE receipt(s) lost on disconnect; "
                         "on-air status is unknown")
        for sock in (self.data_socket, self.kiss_socket, self.command_socket):
            if sock:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
        self.data_socket = self.kiss_socket = self.command_socket = None

    def send(self, payload: bytes, *, priority: int = 0,
             operator_initiated: bool = False) -> None:
        if self.stop_event.is_set() or not self.connected_event.is_set():
            raise ConnectionError("Data2G connection is closed")
        if priority > 0:
            # Bulk history, proofs, and image fragments are repairable by a
            # later sync/request. Never stall the modem receive callback while
            # waiting for room here; its frames keep this queue draining.
            self.bulk_outbound.put_nowait((payload, operator_initiated))
        else:
            # The KISS reader also processes ACKMODE replies. Blocking here
            # could prevent it from freeing the in-flight window.
            self.outbound.put_nowait((payload, operator_initiated))
        self._debug_frames("TX_QUEUE", (payload,))

    def send_operator_initiated(self, payload: bytes, *, priority: int = 0) -> None:
        self.send(payload, priority=priority, operator_initiated=True)

    def _debug_frames(self, event: str, records) -> None:
        if self.on_debug:
            self.on_debug(event, {"records": [unpack(record) for record in records],
                                  "queued": self.outbound.qsize() + self.bulk_outbound.qsize()})

    def discard_queued_transmissions(self) -> int:
        """Drop frames not yet picked up by a transport writer."""
        discarded = 0
        for pending in (self.outbound, self.bulk_outbound):
            while True:
                try:
                    pending.get_nowait()
                    discarded += 1
                except queue.Empty:
                    break
        return discarded

    def connect_peer(self, peer: str) -> None:
        if self.transport != "connected":
            raise RuntimeError("/connect is available with --transport connected")
        peer = peer.upper()
        if not 1 <= len(peer) <= 10 or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/-" for c in peer):
            raise ValueError("peer must be a 1–10 character callsign")
        if not self.connected_event.is_set() or not self.command_socket:
            raise ConnectionError("Data2G command connection is not ready")
        with self.command_lock:
            self.command_socket.sendall(f"CONNECT {self.callsign} {peer}\r".encode("ascii"))

    def send_cqframe(self) -> None:
        """Transmit one Data2G CQ frame so nearby connected stations can be found."""
        if self.transport != "connected":
            raise RuntimeError("/cq is available with --transport connected")
        if not self.connected_event.is_set() or not self.command_socket:
            raise ConnectionError("Data2G command connection is not ready")
        if self.session_event.is_set():
            raise RuntimeError("cannot send CQFRAME during an ARQ session")
        with self.command_lock:
            self.command_socket.sendall(
                f"CQFRAME {self.callsign} {self.bandwidth}\r".encode("ascii"))

    def disconnect_peer(self) -> None:
        if self.transport != "connected" or not self.command_socket:
            raise RuntimeError("/disconnect is available with --transport connected")
        with self.command_lock:
            self.command_socket.sendall(b"DISCONNECT\r")

    def _read_kiss(self):
        parser = KissDecoder()
        try:
            while not self.stop_event.is_set():
                chunk = self.kiss_socket.recv(4096)
                if not chunk:
                    break
                for port, command, payload in parser.feed(chunk):
                    if command == KISS_DATA and (self.on_frame or self.on_frame_context):
                        reception = {}
                        if self.transport == "ax25":
                            if port != self.kiss_tnc_port:
                                continue
                            info = decode_ui_frame(payload)
                            if info is None:
                                continue
                            reception = ui_reception(payload)
                            records = unpack_batch(info) or (info,)
                            port = self.port
                        elif self.transport == "broadcast":
                            records = unpack_batch(payload) or (payload,)
                        else:
                            records = (payload,)
                        for record in records:
                            try:
                                if self.on_frame_context:
                                    self.on_frame_context(port, record, reception)
                                else:
                                    self.on_frame(port, record)
                            except Exception as exc:
                                self._status(f"Ignored invalid KISSChat frame: {exc}")
                    elif command == KISS_ACKMODE and self.transport == "broadcast":
                        if len(payload) == 2:
                            self._ack_kiss_frame(port, payload)
                        else:
                            self._status(f"Malformed Data2G ACKMODE response on port {port}: "
                                         f"expected a 2-byte tag, received {len(payload)} bytes")
        except OSError as exc:
            if not self.stop_event.is_set():
                self._status(f"{self._transport_label()} receive error: {exc}")
        finally:
            if not self.stop_event.is_set():
                self._status(f"{self._transport_label()} connection closed")
                self.stop_event.set()

    def _read_commands(self):
        buffer = bytearray()
        try:
            while not self.stop_event.is_set():
                chunk = self.command_socket.recv(4096)
                if not chunk:
                    break
                buffer.extend(chunk)
                while b"\r" in buffer:
                    raw, _, tail = buffer.partition(b"\r")
                    buffer[:] = tail
                    if raw:
                        line = raw.decode("ascii", "replace").strip()
                        if self.live_command_pending.is_set():
                            self.live_command_responses.put(line)
                            continue
                        if self.transport == "connected" and line.startswith("CONNECTED "):
                            fields = line.split()
                            peer = next((field for field in fields[1:3]
                                         if field.upper() != self.callsign), "peer")
                            self.session_event.set()
                            self._status(f"ARQ connected to {peer} ({fields[-1] if len(fields) > 3 else 'Data2G'})")
                        elif self.transport == "connected" and line.startswith("DISCONNECTED"):
                            self.session_event.clear()
                            self._status(f"ARQ {line.lower()}")
                        elif line != "OK":
                            self._handle_bcast_drop(line)
                            self._status(line)
                if len(buffer) > 16384:
                    self._status("Data2G status line exceeded 16 KiB")
                    buffer.clear()
        except OSError as exc:
            if not self.stop_event.is_set():
                self._status(f"Data2G command connection error: {exc}")
        finally:
            if not self.stop_event.is_set():
                self._status("Data2G command connection closed")
                self.stop_event.set()

    def _read_data(self):
        buffer = bytearray()
        try:
            while not self.stop_event.is_set():
                chunk = self.data_socket.recv(4096)
                if not chunk:
                    break
                buffer.extend(chunk)
                while buffer:
                    length = buffer[0]
                    if length == 0:
                        del buffer[0]
                        continue
                    if len(buffer) < length + 1:
                        break
                    payload = bytes(buffer[1:length + 1])
                    del buffer[:length + 1]
                    if self.on_frame:
                        for record in unpack_batch(payload) or (payload,):
                            try:
                                self.on_frame(self.port, record)
                            except Exception as exc:
                                self._status(f"Ignored invalid ARQ chat record: {exc}")
        except OSError as exc:
            if not self.stop_event.is_set():
                self._status(f"ARQ data receive error: {exc}")
        finally:
            if not self.stop_event.is_set():
                self._status("ARQ data connection closed")
                self.stop_event.set()

    def _write_data(self):
        pending_normal: tuple[bytes, bool] | None = None
        pending_bulk: tuple[bytes, bool] | None = None
        while not self.stop_event.is_set():
            if not self.session_event.wait(0.25):
                continue
            if pending_normal is not None:
                item, pending_normal, from_bulk = pending_normal, None, False
            else:
                try:
                    item = self.outbound.get_nowait()
                    from_bulk = False
                except queue.Empty:
                    if pending_bulk is not None:
                        item, pending_bulk, from_bulk = pending_bulk, None, True
                    else:
                        try:
                            item = self.bulk_outbound.get(timeout=0.25)
                            from_bulk = True
                        except queue.Empty:
                            continue
            if item is None:
                return
            item, _operator_initiated = item
            source_queue = self.bulk_outbound if from_bulk else self.outbound
            records = [item]
            while True:
                try:
                    queued = source_queue.get_nowait()
                except queue.Empty:
                    break
                if queued is None:
                    break
                queued_payload, _queued_operator = queued
                if pack_batch(records + [queued_payload], max_bytes=255) is None:
                    if from_bulk:
                        pending_bulk = queued
                    else:
                        pending_normal = queued
                    break
                records.append(queued_payload)
            payload = pack_batch(records, max_bytes=255) or item
            if len(payload) > 255:
                self._status("ARQ record exceeds Data2G's 255-byte stream limit")
                continue
            try:
                self._set_tx_state("writing to Data2G")
                self.data_socket.sendall(bytes((len(payload),)) + payload)
                self._debug_frames("TX_SUBMIT", records)
                with self.tx_state_lock:
                    self.tx_last_submit = time.monotonic()
                self._set_tx_state("idle")
            except OSError as exc:
                self._set_tx_state("write failed")
                self._status(f"ARQ transmit error: {exc}")
                self.stop_event.set()
                return

    def _write_kiss(self):
        next_ax25_tx = 0.0
        pending_normal: tuple[bytes, bool] | None = None
        pending_bulk: tuple[bytes, bool] | None = None
        last_normal_tag: bytes | None = None
        while not self.stop_event.is_set():
            self._apply_policy_requests()
            if pending_normal is not None:
                item, pending_normal, from_bulk = pending_normal, None, False
            else:
                try:
                    item = self.outbound.get_nowait()
                    from_bulk = False
                except queue.Empty:
                    if pending_bulk is not None:
                        item, pending_bulk, from_bulk = pending_bulk, None, True
                    else:
                        try:
                            item = self.bulk_outbound.get(timeout=0.25)
                            from_bulk = True
                        except queue.Empty:
                            continue
            if item is None:
                return
            item, operator_initiated = item
            records = [item]
            try:
                if self.transport in {"ax25", "broadcast"}:
                    if self.transport == "ax25":
                        delay = max(0.0, next_ax25_tx - time.monotonic())
                        if delay and self.stop_event.wait(delay):
                            return
                    elif not from_bulk and last_normal_tag is not None:
                        # Keep interactive traffic out of the modem's transmit
                        # FIFO until the previous normal frame has aired. Any
                        # messages queued meanwhile can then share one frame.
                        self._set_tx_state("waiting for host receipt")
                        self._wait_for_kiss_ack(last_normal_tag)
                        last_normal_tag = None
                        if self.stop_event.is_set():
                            return
                    # Gather short records queued during the previous radio
                    # slot, then fit a compressed bundle into one legal frame.
                    frame_limit = 256 if self.transport == "ax25" else 220
                    records = [item]
                    source_queue = self.bulk_outbound if from_bulk else self.outbound
                    while True:
                        try:
                            queued = source_queue.get_nowait()
                        except queue.Empty:
                            break
                        if queued is None:
                            break
                        queued_payload, queued_operator = queued
                        if queued_operator != operator_initiated:
                            if from_bulk:
                                pending_bulk = queued
                            else:
                                pending_normal = queued
                            break
                        if pack_batch(records + [queued_payload], max_bytes=frame_limit) is None:
                            if from_bulk:
                                pending_bulk = queued
                            else:
                                pending_normal = queued
                            break
                        records.append(queued_payload)
                    info = pack_batch(records, max_bytes=frame_limit) or item
                    if self.transport == "ax25":
                        frame = encode_ui_frame(self.callsign, self.ax25_destination,
                                                self.digipeaters, info)
                        self._set_tx_state("writing to KISS")
                        self.kiss_socket.sendall(
                            encode_kiss(self.kiss_tnc_port, KISS_DATA, frame))
                        with self.tx_state_lock:
                            self.tx_last_submit = time.monotonic()
                        self._set_tx_state("idle")
                        # KISS has no transmit-complete notification. Pace subsequent
                        # submissions by estimated airtime so the TNC queue cannot
                        # turn a history sync into an unbounded on-air burst.
                        on_air_bytes = len(frame) + 4  # FCS and two HDLC flags
                        estimated_airtime = on_air_bytes * 8 * 1.2 / self.ax25_baud
                        next_ax25_tx = (time.monotonic() + estimated_airtime +
                                        self.ax25_interframe_delay)
                    else:
                        desired_mode = (self.manual_mode if operator_initiated
                                        else self.auto_mode) if self.unattended else self.manual_mode
                        if desired_mode and desired_mode != self.current_broadcast_mode:
                            self._set_live_broadcast_mode(desired_mode)
                            self.current_broadcast_mode = desired_mode
                        self._set_tx_state("waiting for ACKMODE slot")
                        tag = self._reserve_kiss_tag()
                        if tag is None:
                            return
                        self._set_tx_state("writing to KISS")
                        self.kiss_socket.sendall(
                            encode_kiss(self.port, KISS_ACKMODE, tag + info))
                        with self.tx_state_lock:
                            self.tx_last_submit = time.monotonic()
                        self._set_tx_state("idle")
                        if not from_bulk:
                            last_normal_tag = tag
                        if operator_initiated and self.unattended and self.auto_mode != desired_mode:
                            self._set_tx_state("restoring unattended Data2G mode")
                            self._wait_for_kiss_ack(tag)
                            last_normal_tag = None
                            self._set_live_broadcast_mode(self.auto_mode)
                            self.current_broadcast_mode = self.auto_mode
                else:
                    self._set_tx_state("waiting for ACKMODE slot")
                    tag = self._reserve_kiss_tag()
                    if tag is None:
                        return
                    self._set_tx_state("writing to KISS")
                    self.kiss_socket.sendall(encode_kiss(self.port, KISS_ACKMODE, tag + item))
                    with self.tx_state_lock:
                        self.tx_last_submit = time.monotonic()
                    self._set_tx_state("idle")
                self._debug_frames("TX_SUBMIT", records)
            except (OSError, RuntimeError, TimeoutError) as exc:
                self._set_tx_state("write failed")
                if self.transport != "ax25":
                    self._forget_kiss_tag(tag)
                self._status(f"KISS transmit error: {exc}")
                self.stop_event.set()
                return

    def _reserve_kiss_tag(self) -> bytes | None:
        with self.ack_condition:
            while not self.stop_event.is_set():
                now = time.monotonic()
                expired = [tag for tag, sent_at in self.pending_acks.items()
                           if now - sent_at >= KISS_ACK_TIMEOUT]
                for tag in expired:
                    self.pending_acks.pop(tag, None)
                    try:
                        self.ack_order.remove(tag)
                    except ValueError:
                        pass
                if expired:
                    self._status(f"{len(expired)} KISS ACKMODE receipt(s) timed out; "
                                 "on-air status is unknown")
                if len(self.pending_acks) < KISS_ACK_WINDOW:
                    for _ in range(0x10000):
                        self.tag = (self.tag + 1) & 0xFFFF
                        tag = self.tag.to_bytes(2, "big")
                        if tag not in self.pending_acks:
                            self.pending_acks[tag] = now
                            self.ack_order.append(tag)
                            return tag
                oldest = min(self.pending_acks.values(), default=now)
                wait_for = max(0.05, min(1.0, KISS_ACK_TIMEOUT - (now - oldest)))
                self.ack_condition.wait(wait_for)
        return None

    def _wait_for_kiss_ack(self, tag: bytes) -> None:
        deadline = time.monotonic() + KISS_ACK_TIMEOUT
        timed_out = False
        with self.ack_condition:
            while not self.stop_event.is_set() and tag in self.pending_acks:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.pending_acks.pop(tag, None)
                    try:
                        self.ack_order.remove(tag)
                    except ValueError:
                        pass
                    timed_out = True
                    self.ack_condition.notify_all()
                    break
                self.ack_condition.wait(min(remaining, 1.0))
        if timed_out:
            self._status("KISS ACKMODE receipt timed out; on-air status is unknown")

    def _ack_kiss_frame(self, port: int, tag: bytes) -> None:
        if port != self.port:
            return
        with self.ack_condition:
            if self.pending_acks.pop(tag, None) is not None:
                try:
                    self.ack_order.remove(tag)
                except ValueError:
                    pass
                self.ack_condition.notify()

    def _forget_kiss_tag(self, tag: bytes) -> None:
        with self.ack_condition:
            self.pending_acks.pop(tag, None)
            try:
                self.ack_order.remove(tag)
            except ValueError:
                pass
            self.ack_condition.notify()

    def _handle_bcast_drop(self, line: str) -> None:
        fields = line.split()
        if (len(fields) != 4 or fields[0] != "BCAST" or fields[2] != "DROPPED" or
                self.port is None):
            return
        try:
            port, count = int(fields[1]), int(fields[3])
        except ValueError:
            return
        if port != self.port or count <= 0:
            return
        with self.ack_condition:
            released = 0
            while self.ack_order and released < count:
                # Dropped frames are still queued at the host, so they are
                # more likely to be among the most recently submitted tags.
                tag = self.ack_order.pop()
                if self.pending_acks.pop(tag, None) is not None:
                    released += 1
            self.ack_condition.notify_all()

    def _status(self, message: str):
        if self.on_status:
            try:
                self.on_status(message)
            except Exception:
                pass

    def close(self):
        self.shutdown_event.set()
        self.stop_event.set()
        try:
            self.outbound.put_nowait(None)
        except queue.Full:
            pass
        self._close_sockets()
        if self.supervisor and self.supervisor is not threading.current_thread():
            self.supervisor.join(timeout=5)
        for thread in self.threads:
            if thread is not threading.current_thread():
                thread.join(timeout=1)
