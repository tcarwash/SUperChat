#!/usr/bin/env python3
"""Open three interactive SUperChat clients on a simulated lossy radio mesh.

Requires a graphical session and gnome-terminal, Konsole, or xterm. The clients
use the normal raw-KISS/AX.25 path; this bridge connects their radio sockets to
the same timed mesh model as simulate_mesh.py.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

from simulate_mesh import CALLSIGNS, Mesh, SimRadio, STATIONS
from kisschat.ax25 import decode_ui_frame, unpack_batch
from kisschat.data2g import KISS_DATA, KissDecoder
from kisschat.protocol import unpack


def _stop_client(pid_file: Path, database: Path) -> None:
    """Stop only the app process recorded for this launch and temporary DB."""
    deadline = time.monotonic() + 3
    while not pid_file.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    try:
        pid = int(pid_file.read_text(encoding="ascii").strip())
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
    except (OSError, ValueError):
        pid_file.unlink(missing_ok=True)
        return
    if b"kisschat" not in command or os.fsencode(str(database)) not in command:
        pid_file.unlink(missing_ok=True)
        return
    try:
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            current = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
            if b"kisschat" in current and os.fsencode(str(database)) in current:
                os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    finally:
        pid_file.unlink(missing_ok=True)


class ClientRadio(SimRadio):
    """Mesh radio endpoint backed by one SUperChat raw-KISS TCP connection."""

    def __init__(self, name: str, mesh: Mesh):
        super().__init__(name, "", mesh, 0)
        self.connection: socket.socket | None = None
        self.connection_lock = threading.Lock()

    def send_wire(self, connection: socket.socket) -> None:
        with self.connection_lock:
            if self.connection is not None:
                self.mesh.log(f"[bridge] {self.name} rejected duplicate client connection")
                connection.close()
                return
            self.connection = connection
            self.node_id = self.node_id or "connected"
        decoder = KissDecoder()
        self.mesh.log(f"[bridge] {self.name} connected")
        try:
            while not self.mesh.stopping:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                for port, command, payload in decoder.feed(chunk):
                    if port != 0 or command != KISS_DATA:
                        continue
                    info = decode_ui_frame(payload)
                    if info is None:
                        continue
                    records = unpack_batch(info) or (info,)
                    decoded = [unpack(record) for record in records]
                    for record in decoded:
                        if record:
                            self.mesh.trace("TX", self.name, record)
                    if any(decoded):
                        wire, airtime = self.encode_wire(records)
                        self.mesh.transmit(self, records, wire, airtime)
        except (ConnectionError, OSError) as exc:
            self.mesh.log(f"[bridge] {self.name} socket error: {exc}")
        finally:
            self.mesh.log(f"[bridge] {self.name} disconnected")
            with self.connection_lock:
                if self.connection is connection:
                    self.connection = None
                    self.node_id = ""
            try:
                connection.close()
            except OSError:
                pass

    def receive(self, payload: bytes) -> None:
        with self.connection_lock:
            connection = self.connection
        if connection:
            try:
                connection.sendall(payload)
            except OSError:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loss", type=float, default=0.10)
    parser.add_argument("--loss-model", choices=("independent", "bursty"),
                        default="independent")
    parser.add_argument("--burst-recovery", type=float, default=0.5)
    parser.add_argument("--bitrate", type=int, default=1200)
    parser.add_argument("--propagation", type=float, default=0.05)
    parser.add_argument("--interframe", type=float, default=0.25)
    parser.add_argument("--framing-bytes", type=int, default=24)
    parser.add_argument("--contention-window", type=float, default=0.5)
    parser.add_argument("--time-scale", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--topology", choices=("full", "line", "asymmetric"),
                        default="full")
    parser.add_argument("--base-port", type=int, default=18901)
    parser.add_argument("--debug-log", help="write mesh trace here and client traces beside it")
    parser.add_argument("--terminal", choices=("auto", "gnome-terminal", "konsole", "xterm"),
                        default="auto", help="terminal emulator used for each client window")
    opts = parser.parse_args()
    if opts.debug_log:
        opts.debug_log = str(Path(opts.debug_log).expanduser().resolve())
    if not 0 <= opts.loss <= 1 or not 0 < opts.burst_recovery <= 1:
        parser.error("loss must be 0..1 and burst-recovery must be in (0, 1]")
    if not 1 <= opts.base_port <= 65533:
        parser.error("base-port must leave room for three ports")
    if not 0 < opts.time_scale <= 10 or opts.bitrate <= 0:
        parser.error("time-scale must be in (0, 10] and bitrate positive")
    candidates = ([opts.terminal] if opts.terminal != "auto" else
                  ["gnome-terminal", "konsole", "xterm"])
    terminal = next((candidate for candidate in candidates
                     if subprocess.run(["sh", "-c", f"command -v {candidate}"],
                                       capture_output=True).returncode == 0), None)
    if not terminal:
        parser.error("no supported terminal emulator found (gnome-terminal, konsole, xterm)")

    mesh = Mesh(loss=opts.loss, bitrate=opts.bitrate,
                propagation=opts.propagation, interframe=opts.interframe,
                framing_bytes=opts.framing_bytes,
                contention_window=opts.contention_window, time_scale=opts.time_scale,
                seed=opts.seed, topology=opts.topology, loss_model=opts.loss_model,
                burst_recovery=opts.burst_recovery, transport="ax25",
                drop_first_history_fragment=False,
                drop_first_history_page_end=False, drop_first_image_fragment=False,
                drop_first_proof_fragment=False, debug_log=opts.debug_log,
                trace_sync=bool(opts.debug_log))
    stop_reason = "unexpected exit"

    def stop_on_term(signum, _frame):
        raise KeyboardInterrupt(f"received {signal.Signals(signum).name}")

    signal.signal(signal.SIGTERM, stop_on_term)
    if opts.debug_log:
        log_path = Path(opts.debug_log).expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        for path in (log_path, *(log_path.with_name(
                f"{log_path.stem}-{name}{log_path.suffix or '.log'}")
                for name in STATIONS)):
            path.write_text("", encoding="utf-8")
    radios = {name: ClientRadio(name, mesh) for name in STATIONS}
    listeners: dict[str, socket.socket] = {}
    clients: list[subprocess.Popen] = []
    with tempfile.TemporaryDirectory(prefix="superchat-interactive-mesh-") as db_dir:
        database_paths = {name: Path(db_dir) / f"{name}.sqlite3" for name in STATIONS}
        pid_paths = {name: Path(db_dir) / f"{name}.pid" for name in STATIONS}
        try:
            mesh.log(f"[bridge] started topology={opts.topology} loss={opts.loss:.0%} "
                     f"seed={opts.seed} pid={os.getpid()}")
            for index, name in enumerate(STATIONS):
                server = socket.socket()
                server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server.bind(("127.0.0.1", opts.base_port + index))
                server.listen(1)
                server.settimeout(0.5)
                listeners[name] = server

                def accept_client(station=name, listener=server):
                    while not mesh.stopping:
                        try:
                            connection, _ = listener.accept()
                        except socket.timeout:
                            continue
                        except OSError:
                            return
                        threading.Thread(target=radios[station].send_wire,
                                         args=(connection,), daemon=True).start()

                threading.Thread(target=accept_client, daemon=True).start()

            repo = Path(__file__).resolve().parents[1]
            env = os.environ.copy()
            env["PYTHONPATH"] = str(repo) + os.pathsep + env.get("PYTHONPATH", "")
            commands = {}
            for i, (name, nick) in enumerate(zip(STATIONS, ("Alice", "Bob", "Casey"))):
                args = [sys.executable, "-m", "kisschat",
                        "--host", "127.0.0.1", "--transport", "ax25",
                        "--kiss-port", str(opts.base_port + i), "--callsign", CALLSIGNS[name],
                        "--nick", nick, "--db", str(database_paths[name])]
                if opts.debug_log:
                    log_path = Path(opts.debug_log).expanduser().resolve()
                    args += ["--debug-log", str(log_path.with_name(
                        f"{log_path.stem}-{name}{log_path.suffix or '.log'}"))]
                commands[name] = args
            for name in STATIONS:
                cmd = commands[name]
                if terminal == "gnome-terminal":
                    launch = [terminal, "--title", f"SUperChat {name}", "--",
                              sys.executable, str(Path(__file__).with_name(
                                  "run_interactive_client.py")), str(pid_paths[name]), *cmd]
                elif terminal == "konsole":
                    launch = [terminal, "--new-window", "-p",
                              f"tabtitle=SUperChat {name}", "-e", sys.executable,
                              str(Path(__file__).with_name("run_interactive_client.py")),
                              str(pid_paths[name]), *cmd]
                else:
                    launch = [terminal, "-T", f"SUperChat {name}", "-e", sys.executable,
                              str(Path(__file__).with_name("run_interactive_client.py")),
                              str(pid_paths[name]), *cmd]
                clients.append(subprocess.Popen(launch, env=env, cwd=repo))
            print(f"3 client windows launched; topology={opts.topology}, loss={opts.loss:.0%} "
                  f"({opts.loss_model}), seed={opts.seed}")
            print("Close the client windows or press Ctrl-C here to stop the mesh.")
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt as exc:
            stop_reason = str(exc) or "Ctrl-C"
        except BaseException as exc:
            stop_reason = f"{type(exc).__name__}: {exc}"
            mesh.log(f"[bridge] exiting on error: {stop_reason}")
            raise
        finally:
            mesh.log(f"[bridge] stopping: {stop_reason}")
            for name in STATIONS:
                _stop_client(pid_paths[name], database_paths[name])
                if pid_paths[name].exists():
                    mesh.log(f"[bridge] could not stop tracked client {name}")
            mesh.close()
            for listener in listeners.values():
                listener.close()
            for client in clients:
                client.terminate()
            for radio in radios.values():
                radio.close()
            mesh.log("[bridge] stopped")


if __name__ == "__main__":
    main()
