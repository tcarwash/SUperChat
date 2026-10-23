"""Channel chat, store-and-forward history sync, and terminal UI."""
from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import json
import os
import queue
import re
import secrets
import signal
import shlex
import threading
import time

from . import __version__
from .auth import ARAPAuth, ARAPUnavailable
from .data2g import Data2G
from .protocol import (MAX_FRAME, MAX_MESSAGE_BYTES, MAX_PARTS,
                       MAX_SPARSE_SEQUENCE_REQUESTS, image_part, image_parts,
                       message_parts, relay_message_part,
                       relay_proof_part, pack, unpack)
from .store import Store
from .images import make_thumbnail
from .pixel_cards import (PixelCard, decode_pixel_card_message,
                          decode_pixel_card_reference, encode_pixel_card_reference,
                          load_pixel_card, save_received_pixel_card_message,
                          verify_pixel_card_payload)
from .mentions import MENTION, format_mention, mention_name
import hashlib
from datetime import datetime, timezone


CHANNEL = re.compile(r"^#[A-Za-z0-9_-]{1,31}$")
NODE_ID = re.compile(r"^[0-9a-f]{32}$")
SYNC_PAGE_MESSAGES = 8
SYNC_SPARSE_MESSAGES = MAX_SPARSE_SEQUENCE_REQUESTS
SYNC_PAGE_REPAIR_LIMIT = 3
AUTH_CHALLENGE_CHUNK_BYTES = 112
AUTH_CHALLENGE_MAX_BYTES = 4096
# A lost broadcast request or page end should recover on a useful chat
# timescale. Retries are bounded and back off, so this does not become a
# continuous source of radio traffic.
SYNC_REQUEST_RETRY_SECONDS = 12
MAX_SYNC_REQUEST_RETRIES = 3
SYNC_HISTORY_SECONDS = 48 * 60 * 60
DEFAULT_SYNC_HISTORY_HOURS = SYNC_HISTORY_SECONDS // 3600
MAX_SYNC_HISTORY_HOURS = 24 * 3650
HEARD_PEER_TTL = 15 * 60
MAX_HEARD_PEERS = 512
MAX_ANNOUNCED_ORIGINS = 8192
MAX_INVENTORY_FRAMES_PER_ANNOUNCE = 96
MAX_PENDING_SYNC_REQUESTS = 4096
MAX_COMPLETED_SYNC_WINDOWS = 4096
SYNC_WINDOW_RETRY_SECONDS = 300
RELAY_ANNOUNCE_JITTER_SECONDS = 8
SLOW_RELAY_ANNOUNCE_INTERVAL_SECONDS = 30
MAX_IMAGE_REQUESTS = 4096
MAX_IMAGE_CHUNKS_SEEN = 4096
MAX_IMAGE_RESPONSE_TIMERS = 512
MAX_PENDING_IMAGES = 512
IMAGE_REQUEST_WINDOW = 8
IMAGE_RESPONSE_SECONDS = 45
# Reset this silence timer on claims and chunks within each bounded turn.
IMAGE_RETRY_SECONDS = 20
# Bound the fast repair path to one recent short message per response.
LIVE_MESSAGE_REPAIR_SECONDS = 120
MAX_PENDING_AUTH_JOBS = 128
MAX_SEEN_RECEIPT_ACKS = 8192
MAX_UI_EVENTS = 1024


class ChatEventQueue:
    """Bound async UI notifications and request a durable-state refresh on overflow."""

    def __init__(self, maxsize: int = MAX_UI_EVENTS):
        self.maxsize = maxsize
        self.items = deque()
        self.lock = threading.Lock()

    def put(self, item: tuple[str, object]) -> None:
        with self.lock:
            if len(self.items) >= self.maxsize:
                self.items.clear()
                self.items.append(("refresh", None))
                return
            self.items.append(item)

    def get_nowait(self) -> tuple[str, object]:
        with self.lock:
            if not self.items:
                raise queue.Empty
            return self.items.popleft()


class Chat:
    def __init__(self, args):
        self.args = args
        self.debug_log_lock = threading.Lock()
        # Interactive clients periodically repair messages missed during
        # simultaneous radio transmissions. Operators can disable that airtime
        # use explicitly; headless relays always keep history moving.
        self.auto_sync = bool(getattr(args, "auto_sync", True) or
                              getattr(args, "headless", False))
        callsign = str(args.callsign).upper()
        if (not 1 <= len(callsign) <= 10 or
                any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/-"
                    for char in callsign)):
            raise ValueError("callsign must be 1–10 characters from A–Z, 0–9, /, -")
        command_port = getattr(args, "command_port", 8300)
        kiss_port = getattr(args, "kiss_port", None)
        if kiss_port is None:
            kiss_port = 8001 if getattr(args, "transport", "broadcast") == "ax25" else 8100
        if type(command_port) is not int or not 1 <= command_port <= 65535:
            raise ValueError("command port must be between 1 and 65535")
        if type(kiss_port) is not int or not 1 <= kiss_port <= 65535:
            raise ValueError("KISS port must be between 1 and 65535")
        if (getattr(args, "transport", "broadcast") == "connected" and
                command_port == 65535):
            raise ValueError("connected transport needs command port + 1 for data")
        os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
        self.store = Store(args.db, args.callsign)
        self.sync_history_hours = self._load_sync_setting(
            args, "sync_history_hours", "sync_history_hours",
            DEFAULT_SYNC_HISTORY_HOURS, MAX_SYNC_HISTORY_HOURS, minimum=1)
        history_message_limit = self._load_sync_setting(
            args, "sync_history_messages", "sync_history_messages", 0, 1_000_000)
        self.sync_history_messages = history_message_limit or None
        self.sync_history_budget_remaining = self._sync_exchange_budget()
        self.sync_history_budget_generation = 0
        saved_unattended = self.store.saved_setting("unattended")
        self.unattended = (True if getattr(args, "unattended", False) else
                           saved_unattended == "1" if saved_unattended is not None else False)
        self.events = ChatEventQueue()
        self.channel = (getattr(args, "channel", None) or self.store.saved_channel()
                        or "#general")
        if not CHANNEL.fullmatch(self.channel):
            raise ValueError("channel must look like #general (letters, digits, _ and -)")
        self.store.save_channel(self.channel)
        self.nick = args.nick or self.store.saved_nick() or args.callsign.upper()
        if (not self.nick or len(self.nick.encode("utf-8")) > 16 or
                not all(char.isprintable() for char in self.nick)):
            raise ValueError("nick must be printable and 1–16 UTF-8 bytes")
        self.store.save_nick(self.nick)
        self.auth = ARAPAuth(cli=args.arap_cli, callsign=args.callsign,
                             key=args.arap_key, key_dir=args.arap_key_dir,
                             trust_store=args.arap_trust_store,
                             passphrase_env=args.arap_passphrase_env,
                             servers=getattr(args, "arap_server", []))
        if self.auth.servers:
            self.events.put(("status", "ARAP out-of-band proofs enabled: signing uploads "
                            "proofs and verification fetches them over the network; only "
                            "AR1 references are sent over radio"))
        self.auth_executor = ThreadPoolExecutor(max_workers=1,
                                                thread_name_prefix="kisschat-arap")
        self.image_executor = ThreadPoolExecutor(max_workers=1,
                                                 thread_name_prefix="kisschat-image")
        self.image_slots = threading.BoundedSemaphore(2)
        self.auth_pending: dict[tuple[str, int], tuple[bytes, str]] = {}
        self.auth_checked: dict[tuple[str, int], tuple[bytes, str]] = {}
        self.signing_jobs: set[tuple[str, int]] = set()
        self.reference_jobs: set[tuple[str, int]] = set()
        self.signing_retry_after: dict[tuple[str, int], float] = {}
        self.auth_pending_lock = threading.RLock()
        self.auth_reverify_cursor: tuple[str, int] | None = None
        self.auth_request_dir = os.path.abspath(args.db) + ".auth-requests"
        self.auth_challenge_store = os.path.join(self.auth_request_dir, "challenges.sqlite3")
        self.auth_fragment_lock = threading.RLock()
        self.auth_fragments: dict[tuple[str, str], dict] = {}
        self.auth_response_cooldown: dict[str, float] = {}
        self.auth_policy_revision = self.auth.verification_revision()
        self.store.refresh_auth_policy(self.auth_policy_revision)
        self.peer_names: dict[str, str] = {}
        self.peer_nicks: dict[str, str] = {}
        self.peer_last_seen: dict[str, float] = {}
        self.cq_frames: dict[str, float] = {}
        self.peer_lock = threading.RLock()
        self.pending: dict[tuple[str, str, int], float] = {}
        self.pending_sync_requests: dict[
            tuple[str, str, int], tuple[dict, tuple, float, int]
        ] = {}
        self.completed_sync_windows: dict[tuple[str, int, int], float] = {}
        self.pending_sync_windows: dict[tuple[str, int, int], tuple[str, float]] = {}
        self.served_sync_requests: dict[tuple, float] = {}
        self.sync_repair_attempts: dict[tuple[str, str, int], int] = {}
        self.pending_lock = threading.RLock()
        self.seen_image_requests: dict[tuple[str, str, int, str, str], tuple[int, float]] = {}
        self.image_claims: dict[tuple[str, str, int, str, str], tuple[str, float]] = {}
        self.image_response_timers: dict[tuple[str, str, int, str, str], threading.Timer] = {}
        self.seen_image_chunks: dict[tuple[str, str, str, int, str, int], tuple[int, float]] = {}
        # A manual attachment request authorizes bounded retries for missing chunks.
        # The first request remains explicit; retries never start for an image
        # the operator has not selected.
        self.pending_images: dict[tuple[str, int, str], tuple[int, float, float]] = {}
        self.image_windows: dict[tuple[str, int, str], set[int]] = {}
        self.image_lock = threading.RLock()
        self.seen_routes: dict[tuple, tuple[int, float]] = {}
        self.route_lock = threading.RLock()
        self.pending_message_relays: dict[tuple, tuple[dict, int, float]] = {}
        self.message_relay_lock = threading.RLock()
        self.pending_end_relays: dict[tuple, tuple[dict, int, float]] = {}
        self.end_relay_lock = threading.RLock()
        self.seen_receipt_acks: dict[tuple[str, int, str, str], float] = {}
        self.last_announced: dict[str, float] = {}
        self.pending_announcements: dict[str, float] = {}
        self.last_slow_relay_announcement = 0.0
        self.announce_lock = threading.Lock()
        self.inventory_cursor = 0
        self.attachment_cursor: tuple[str, int] = ("", 0)
        self.inventory_lock = threading.Lock()
        self.sync_stop = threading.Event()
        self.sync_thread: threading.Thread | None = None
        self.transport_connected_once = False
        self.pending_local_delivery = threading.Event()
        self.data = Data2G(args.host, args.callsign, command_port=args.command_port,
                           kiss_port=kiss_port, mode=args.mode,
                           transport=getattr(args, "transport", "broadcast"),
                           bandwidth=getattr(args, "bandwidth", "2300"),
                           max_bandwidth=getattr(args, "max_bandwidth", None),
                           ax25_destination=getattr(args, "ax25_destination", "CQ"),
                           digipeaters=tuple(filter(None, getattr(args, "digipeaters", "").split(","))),
                           kiss_tnc_port=getattr(args, "kiss_tnc_port", 0),
                           ax25_baud=getattr(args, "ax25_baud", 1200),
                           ax25_interframe_delay=getattr(args, "ax25_interframe_delay", 0.25),
                           unattended=self.unattended,
                           on_frame=self._on_frame,
                           on_frame_context=self._on_frame,
                           on_status=self._on_data_status,
                           on_debug=self._debug_log if getattr(args, "debug_log", "") else None)

    def station_names(self) -> list[str]:
        with self.peer_lock:
            return sorted(set(self.peer_names.values()) | set(self.peer_nicks.values()) |
                          set(self.cq_frames),
                          key=str.casefold)

    def station_labels(self) -> list[tuple[str, str]]:
        with self.peer_lock:
            labels = {callsign.casefold(): (self.peer_nicks.get(peer, callsign), callsign)
                      for peer, callsign in self.peer_names.items()}
            for callsign in self.cq_frames:
                labels.setdefault(callsign.casefold(), (callsign, callsign))
        for callsign in self.store.authenticated_stations():
            labels.setdefault(callsign.casefold(), (callsign, callsign))
        return sorted(labels.values(), key=lambda item: item[0].casefold())

    def station_count(self) -> int:
        with self.peer_lock:
            return len({callsign.casefold() for callsign in self.peer_names.values()} |
                       {callsign.casefold() for callsign in self.cq_frames})

    def start(self):
        self._recover_mention_receipts()
        self._retry_pending_verifications()
        self.data.start()
        self.sync_thread = threading.Thread(target=self._sync_loop, name="kisschat-sync",
                                           daemon=True)
        self.sync_thread.start()
        self.events.put(("status", f"Connecting to {self.args.host}; "
                         f"station {self.args.callsign}"))

    def _debug_log(self, event: str, detail: object) -> None:
        path = getattr(self.args, "debug_log", "")
        if not path:
            return
        stamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        line = f"{stamp} {self.args.callsign} {event} {detail}\n"
        try:
            with self.debug_log_lock, open(path, "a", encoding="utf-8") as stream:
                stream.write(line)
                stream.flush()
        except OSError as exc:
            # Preserve the diagnostic failure in the terminal instead of
            # silently losing the requested trace.
            print(f"debug log {path}: {exc}", flush=True)

    def _recover_mention_receipts(self):
        """Restore any receipt lost between committing a message and its ack."""
        received_at = int(time.time())
        for message in self.store.messages_with_mentions():
            if message["o"] == self.store.node:
                continue
            for mention in self._mentioned_here(message["b"]):
                self.store.add_relay_receipt(message["o"], int(message["s"]),
                                             self.store.node, self.args.callsign,
                                             mention, received_at)

    def _on_data_status(self, value: str):
        self._debug_log("STATUS", value)
        self.events.put(("status", value))
        connected_status = (value.startswith("Connected to Data2G at ") or
                            value.startswith("Connected to raw KISS TNC at "))
        if connected_status:
            was_connected = self.transport_connected_once
            self.transport_connected_once = True
            if was_connected or self.pending_local_delivery.is_set():
                # A reconnect should repair messages saved while the link was
                # down. A first connection also repairs a message the operator
                # explicitly queued while the transport was unavailable.
                self.sync()
        fields = value.split()
        if (self.args.transport == "connected" and len(fields) == 3 and
                fields[0].upper() == "CQFRAME"):
            callsign = fields[1].upper()
            if (callsign != self.args.callsign.upper() and 1 <= len(callsign) <= 10 and
                    all(char in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/-"
                        for char in callsign)):
                with self.peer_lock:
                    if callsign not in self.cq_frames and len(self.cq_frames) >= MAX_HEARD_PEERS:
                        oldest = min(self.cq_frames, key=self.cq_frames.get)
                        self.cq_frames.pop(oldest, None)
                    self.cq_frames[callsign] = time.monotonic()
        if value.startswith("ARQ connected"):
            self.sync()

    def sync(self, *, retry: bool = False, discover: bool = True):
        """Advertise local history and ask nearby stations for their inventories."""
        if self.args.transport == "connected" and not self.data.session_event.is_set():
            return
        now = time.monotonic()
        with self.pending_lock:
            if retry or (not self.pending_sync_requests and
                         not self.pending_sync_windows):
                self.sync_history_budget_remaining = self._sync_exchange_budget()
                self.sync_history_budget_generation += 1
            if retry:
                # An explicit user sync is also a retry for pages whose query
                # or response was lost. Leave routine/automatic syncs damped.
                self.pending.clear()
                self.pending_sync_requests.clear()
                self.completed_sync_windows.clear()
                self.pending_sync_windows.clear()
                self.sync_repair_attempts.clear()
            for key, expiry in list(self.pending.items()):
                if (expiry <= now and key not in self.pending_sync_requests):
                    self.pending.pop(key, None)
        self._forward_pending_receipts()
        # Repair deferred signatures only when history sync is requested or
        # scheduled. Interactive clients otherwise remain quiet on first connect.
        self._retry_pending_signatures()
        self.announce()
        if discover:
            self._send_routed_record({"t": "w", "p": self.store.node, "v": 2, "ttl": 0},
                                    ("w", self.store.node, 2))
        self.pending_local_delivery.clear()

    def _load_sync_setting(self, args, option: str, key: str, default: int,
                           maximum: int, *, minimum: int = 0) -> int:
        configured = getattr(args, option, None)
        saved = None if configured is not None else self.store.saved_setting(key)
        raw = configured if configured is not None else saved
        if raw is None:
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            if configured is not None:
                raise ValueError(f"{option.replace('_', '-')} must be an integer")
            return default
        if not minimum <= value <= maximum:
            raise ValueError(
                f"{option.replace('_', '-')} must be between {minimum} and {maximum}")
        if configured is not None:
            self.store.save_setting(key, str(value))
        return value

    def _history_since(self, now_epoch: int | None = None) -> int:
        now = int(time.time()) if now_epoch is None else now_epoch
        hours = getattr(self, "sync_history_hours", DEFAULT_SYNC_HISTORY_HOURS)
        return now - hours * 3600

    def _sync_exchange_budget(self) -> int:
        """Cap one exchange at the number of sequence IDs carried in one query."""
        configured = (self.sync_history_messages if self.sync_history_messages is not None
                      else SYNC_SPARSE_MESSAGES)
        return min(configured, SYNC_SPARSE_MESSAGES)

    def _slow_broadcast_mode(self) -> bool:
        """True when the selected Data2G broadcast mode is 500 Hz or narrower."""
        if getattr(self.args, "transport", "broadcast") != "broadcast":
            return False
        selected = (getattr(self.data, "auto_mode", None) if self.unattended else
                    getattr(self.data, "manual_mode", None))
        width = next((width for name, width in getattr(self.data, "mode_catalog", ())
                      if name == selected), None)
        return width is not None and width <= 500

    def _sync_retry_interval(self) -> int:
        configured = getattr(self.args, "sync_retry_seconds", SYNC_REQUEST_RETRY_SECONDS)
        return max(configured, 60) if self._slow_broadcast_mode() else configured

    def _image_retry_interval(self) -> int:
        return max(IMAGE_RETRY_SECONDS, 120) if self._slow_broadcast_mode() else IMAGE_RETRY_SECONDS

    def _sync_loop(self):
        # Randomize the first and later inventory exchanges so a group started
        # together does not transmit in lockstep. Routine sync uses five minutes.
        next_sync = time.monotonic() + 300 + secrets.randbelow(120 + 1)
        next_auth_policy_check = time.monotonic() + 2
        while not self.sync_stop.wait(0.25):
            self._expire_peer_sightings()
            self._flush_relay_announcements()
            self._flush_message_relays()
            self._flush_end_relays()
            self._retry_pending_images()
            self._retry_pending_sync_requests()
            if time.monotonic() >= next_auth_policy_check:
                self._refresh_auth_policy()
                next_auth_policy_check = time.monotonic() + 2
            self._retry_pending_verifications()
            if (self.auto_sync and time.monotonic() >= next_sync and
                    not self._receiving_image_window()):
                self.sync(discover=False)
                next_sync = (time.monotonic() + 300 +
                             secrets.randbelow(120 + 1))

    def send_record(self, record: dict, *, priority: int = 0,
                    operator_initiated: bool = False) -> bool:
        try:
            payload = pack(record)
            return self.send_payload(payload, priority=priority,
                                     operator_initiated=operator_initiated)
        except (ValueError, OSError, queue.Full, ConnectionError) as exc:
            self.events.put(("status", f"Transmit queue: {exc}"))
            return False

    def _message_sender_id(self) -> str | None:
        """Identify the transmitting node where the transport has no source field."""
        return (self.store.node if getattr(self.args, "transport", "broadcast")
                in {"broadcast", "connected"} else None)

    def set_unattended(self, enabled: bool) -> None:
        """Change and persist the live automatic-transmit bandwidth policy."""
        self.data.set_unattended(enabled)
        self.unattended = bool(enabled)
        self.args.unattended = self.unattended
        self.store.save_setting("unattended", "1" if self.unattended else "0")
        self._debug_log("BANDWIDTH_POLICY_UI", {"unattended": self.unattended})
        self.events.put(("status", "Unattended bandwidth policy " +
                         ("enabled" if self.unattended else "disabled")))

    def send_payload(self, payload: bytes, *, priority: int = 0,
                     operator_initiated: bool = False) -> bool:
        try:
            if len(payload) > MAX_FRAME:
                raise ValueError("control record too large")
            if operator_initiated and hasattr(self.data, "send_operator_initiated"):
                self.data.send_operator_initiated(payload, priority=priority)
            else:
                self.data.send(payload, priority=priority)
            return True
        except (ValueError, OSError, queue.Full, ConnectionError) as exc:
            self.events.put(("status", f"Transmit queue: {exc}"))
            return False

    def announce(self, *, legacy: bool = False):
        with self.inventory_lock:
            self._announce_inventory(legacy=legacy)
            self._announce_attachment_metadata()

    def _announce_attachment_metadata(self, origin: str | None = None) -> None:
        since = self._history_since()
        after = self.attachment_cursor if origin is None else ("", 0)
        attachment = self.store.next_attachment_announcement(since, origin=origin, after=after)
        if attachment is None and origin is None and after != ("", 0):
            attachment = self.store.next_attachment_announcement(since)
        if attachment is None:
            return
        # Neighbor caches advertise their metadata in turn. No repeated flood
        # and no attachment payload is needed to repair an orphaned caption.
        sent = self.send_record({"t": "a", "o": attachment["origin"],
                                 "s": attachment["seq"], "h": attachment["digest"],
                                 "b": attachment["size"], "w": attachment["width"],
                                 "hgt": attachment["height"], "ttl": 0},
                                priority=10 if origin is None else 0)
        if sent and origin is None:
            self.attachment_cursor = (attachment["origin"], int(attachment["seq"]))

    def _proof_mask(self, origins) -> int:
        return sum(1 << index for index, origin in enumerate(origins)
                   if (origin == self.store.node and self.auth.enabled) or
                   self.store.has_proofs(origin))

    def _announce_inventory(self, *, legacy: bool = False):
        since = self._history_since()
        inventory = self.store.inventory(since)
        if not inventory:
            return
        start = self.inventory_cursor % len(inventory)
        inventory = inventory[start:] + inventory[:start]
        frame_budget = 16 if self._slow_broadcast_mode() else MAX_INVENTORY_FRAMES_PER_ANNOUNCE
        sent_entries = 0
        if legacy:
            for origin, low, high in inventory[:frame_budget]:
                sent = self.send_record({"t": "i", "p": self.store.node,
                                         "n": self.args.callsign, "nick": self.nick,
                                         "o": origin, "a": low, "m": high, "ttl": 4,
                                         "sig": self._proof_mask([origin])},
                                        priority=10)
                if not sent:
                    break
                sent_entries += 1
            self.inventory_cursor = (start + sent_entries) % len(inventory)
            return
        batch: list[list[str | int]] = []
        lows: list[int] = []
        frames_sent = 0

        def flush_batch() -> bool:
            nonlocal batch, lows, frames_sent, sent_entries
            if not batch:
                return True
            if frames_sent >= frame_budget:
                return False
            sent = self.send_record({"t": "i", "p": self.store.node,
                                     "n": self.args.callsign, "nick": self.nick,
                                     "v": batch, "l": lows, "ttl": 4,
                                     "sig": self._proof_mask(item[0] for item in batch)}, priority=10)
            if not sent:
                return False
            sent_entries += len(batch)
            frames_sent += 1
            batch = []
            lows = []
            return True

        stopped_on_queue = False
        for origin, low, high in inventory:
            candidate = batch + [[origin, high]]
            candidate_lows = lows + [low]
            record = {"t": "i", "p": self.store.node,
                      "n": self.args.callsign, "nick": self.nick,
                      "v": candidate, "l": candidate_lows, "ttl": 4,
                      "sig": self._proof_mask(item[0] for item in candidate)}
            try:
                pack(record)
            except ValueError:
                if not flush_batch():
                    stopped_on_queue = True
                    break
                batch = [[origin, high]]
                lows = [low]
                if frames_sent >= frame_budget:
                    break
            else:
                batch = candidate
                lows = candidate_lows
        if not stopped_on_queue:
            flush_batch()
        self.inventory_cursor = (start + sent_entries) % len(inventory)

    def announce_origin(self, origin: str, *, force: bool = False,
                        high: int | None = None, compact: bool = False):
        since = self._history_since()
        bounds = self.store.recent_bounds(origin, since)
        if bounds is None:
            return True
        low, recent_high = bounds
        high = recent_high
        now = time.monotonic()
        with self.announce_lock:
            if not force and now - self.last_announced.get(origin, 0) < 8:
                return False
            self.last_announced.pop(origin, None)
            self.last_announced[origin] = now
            while len(self.last_announced) > MAX_ANNOUNCED_ORIGINS:
                self.last_announced.pop(next(iter(self.last_announced)))
        if high is None:
            high = self.store.highwater(origin)
        proof_mask = self._proof_mask([origin])
        advertised_low = low
        if compact and not proof_mask:
            # Compact inventory bounds let peers compare their contiguous
            # sequence with the latest one we have and request only the gap.
            sent = self.send_record({"t": "k", "from": self.store.node,
                                     "h": [[origin, advertised_low, high]]})
        else:
            sent = self.send_record({"t": "i", "p": self.store.node,
                                     "n": self.args.callsign, "nick": self.nick,
                                     "o": origin, "a": advertised_low, "m": high,
                                     "ttl": 4, "sig": proof_mask})
        if not sent:
            with self.announce_lock:
                self.last_announced.pop(origin, None)
        else:
            self._debug_log("ORIGIN_ANNOUNCE", {"o": origin, "low": advertised_low,
                                                 "high": high,
                                                 "compact": compact and not proof_mask})
            self._announce_attachment_metadata(origin)
        return sent

    def _queue_relay_announcement(self, origin: str) -> None:
        """Coalesce heard history by origin and jitter relay inventory airtime."""
        now = time.monotonic()
        with self.announce_lock:
            if origin in self.pending_announcements:
                return
            if len(self.pending_announcements) >= MAX_ANNOUNCED_ORIGINS:
                oldest = min(self.pending_announcements,
                             key=self.pending_announcements.get)
                self.pending_announcements.pop(oldest, None)
            jitter_ms = secrets.randbelow(RELAY_ANNOUNCE_JITTER_SECONDS * 1000 + 1)
            self.pending_announcements[origin] = now + jitter_ms / 1000

    def _flush_relay_announcements(self, *, image_boundary: bool = False) -> None:
        receiving_image = not image_boundary and self._receiving_image_window()
        now = time.monotonic()
        due = []
        with self.announce_lock:
            for origin, deadline in list(self.pending_announcements.items()):
                # An image must not hide recovery of a local chat message
                # that collided. Background cache announcements still wait
                # for a window boundary, and the normal rate limit applies.
                if receiving_image and origin != self.store.node:
                    continue
                if deadline > now:
                    continue
                if self._slow_broadcast_mode():
                    available_at = (self.last_slow_relay_announcement +
                                    SLOW_RELAY_ANNOUNCE_INTERVAL_SECONDS)
                    if now < available_at:
                        jitter = secrets.randbelow(RELAY_ANNOUNCE_JITTER_SECONDS * 1000 + 1) / 1000
                        self.pending_announcements[origin] = available_at + jitter
                        continue
                last = self.last_announced.get(origin, 0)
                if now - last < 8:
                    jitter_ms = secrets.randbelow(RELAY_ANNOUNCE_JITTER_SECONDS * 1000 + 1)
                    self.pending_announcements[origin] = last + 8 + jitter_ms / 1000
                    continue
                self.pending_announcements.pop(origin, None)
                due.append(origin)
                if self._slow_broadcast_mode():
                    self.last_slow_relay_announcement = now
        for origin in due:
            if not self.announce_origin(origin, compact=origin == self.store.node):
                with self.announce_lock:
                    retry_ms = secrets.randbelow(RELAY_ANNOUNCE_JITTER_SECONDS * 1000 + 1)
                    self.pending_announcements[origin] = (
                        time.monotonic() + 2 + retry_ms / 1000)

    def _route_once(self, key: tuple, ttl: int) -> bool:
        if ttl <= 0:
            return False
        now = time.monotonic()
        with self.route_lock:
            previous, expiry = self.seen_routes.get(key, (-1, 0))
            if expiry > now and ttl <= previous:
                return False
            # History and proof requests are retried on a shorter cadence than
            # this cache horizon. Keep duplicate suppression long enough to
            # stop mesh loops, but let a later repair request traverse relays.
            self.seen_routes[key] = (ttl, now + 30)
            if len(self.seen_routes) > 8192:
                self.seen_routes = {item: state for item, state in self.seen_routes.items()
                                    if state[1] > now}
                while len(self.seen_routes) > 8192:
                    self.seen_routes.pop(next(iter(self.seen_routes)))
        return True

    def _queue_message_relay(self, key: tuple, record: dict, ttl: int) -> None:
        # A short randomized holdoff lets nearby relays hear one another and
        # suppress duplicate copies before they consume another radio slot.
        due = time.monotonic() + 0.35 + secrets.randbelow(1400) / 1000
        with self.message_relay_lock:
            if len(self.pending_message_relays) >= 4096:
                oldest = min(self.pending_message_relays,
                             key=lambda item: self.pending_message_relays[item][2])
                self.pending_message_relays.pop(oldest, None)
            self.pending_message_relays[key] = (dict(record), ttl, due)

    def _flush_message_relays(self) -> None:
        now = time.monotonic()
        with self.message_relay_lock:
            due = [(key, item) for key, item in self.pending_message_relays.items()
                   if item[2] <= now]
            for key, _ in due:
                self.pending_message_relays.pop(key, None)
        for _, (record, ttl, _) in due:
            sender = self._message_sender_id() if record.get("from") else None
            self.send_payload(relay_message_part(record, ttl - 1, sender=sender),
                              priority=10)

    def _queue_end_relay(self, key: tuple, record: dict, ttl: int) -> None:
        # The page-end record is not a receipt. Let delayed fragment relays
        # leave first so the requester is less likely to ask for frames that
        # are already on their way.
        due = time.monotonic() + 2.0
        with self.end_relay_lock:
            if len(self.pending_end_relays) >= 512:
                oldest = min(self.pending_end_relays,
                             key=lambda item: self.pending_end_relays[item][2])
                self.pending_end_relays.pop(oldest, None)
            self.pending_end_relays[key] = (dict(record), ttl, due)

    def _flush_end_relays(self) -> None:
        now = time.monotonic()
        with self.end_relay_lock:
            due = [(key, item) for key, item in self.pending_end_relays.items()
                   if item[2] <= now]
            for key, _ in due:
                self.pending_end_relays.pop(key, None)
        for _, (record, ttl, _) in due:
            forwarded = dict(record)
            forwarded["ttl"] = ttl - 1
            self.send_record(forwarded, priority=10)

    def _suppress_duplicate_message_relay(self, key: tuple, ttl: int) -> None:
        now = time.monotonic()
        with self.route_lock:
            previous, expiry = self.seen_routes.get(key, (-1, 0))
        # A smaller TTL means another relay has already forwarded this part.
        if expiry > now and ttl < previous:
            with self.message_relay_lock:
                self.pending_message_relays.pop(key, None)

    def _allow_message_repair(self, recipient: str, origin: str,
                              sequences: list[int]) -> None:
        """Let an explicit page repair pass through caches from the first attempt."""
        sequence_set = set(sequences)
        with self.route_lock:
            for key in list(self.seen_routes):
                if (len(key) == 5 and key[0] == "d" and key[1] == recipient and
                        key[2] == origin and key[3] in sequence_set):
                    self.seen_routes.pop(key, None)
        with self.message_relay_lock:
            for key in list(self.pending_message_relays):
                if (key[1] == recipient and key[2] == origin and
                        key[3] in sequence_set):
                    self.pending_message_relays.pop(key, None)

    @staticmethod
    def _track_pending(pending: dict, key: tuple, now: float, timeout: float = 90) -> bool:
        """Track retryable sync work with bounded lifetime and memory use."""
        for old_key, expiry in list(pending.items()):
            if expiry <= now:
                pending.pop(old_key, None)
        if pending.get(key, 0) > now:
            return False
        if key not in pending and len(pending) >= MAX_PENDING_SYNC_REQUESTS:
            pending.pop(min(pending, key=pending.get), None)
        pending[key] = now + timeout
        return True

    def _claim_sync_response(self, key: tuple, now: float) -> bool:
        """Coalesce repeated copies of one request while allowing its retry."""
        retry_seconds = self._sync_retry_interval()
        holdoff = max(0.25, min(2.0, retry_seconds / 2))
        with self.pending_lock:
            for old_key, expiry in list(self.served_sync_requests.items()):
                if expiry <= now:
                    self.served_sync_requests.pop(old_key, None)
            if self.served_sync_requests.get(key, 0) > now:
                return False
            if len(self.served_sync_requests) >= MAX_PENDING_SYNC_REQUESTS:
                oldest = min(self.served_sync_requests,
                             key=self.served_sync_requests.get)
                self.served_sync_requests.pop(oldest, None)
            self.served_sync_requests[key] = now + holdoff
            return True

    def _send_sync_request(self, record: dict, key: tuple, route: tuple) -> bool:
        """Queue a page query and retain it for bounded timeout retries."""
        record = dict(record)
        if not record.get("v") and not record.get("j") and not record.get("r"):
            # KC2H already carries ordinary windows with an empty repair list.
            # Avoid a ~200-byte JSON query for a ~74-byte binary request.
            record.setdefault("h", [])
        now = time.monotonic()
        retry_seconds = self._sync_retry_interval()
        # Hidden stations hear the same inventory at the same instant. Their
        # retries must not stay synchronized after the first collision.
        retry_delay = retry_seconds * (1 + secrets.randbelow(1001) / 2000)
        retry_at = now + retry_delay
        with self.pending_lock:
            # A due retry is still active work. Generic TTL pruning used to
            # delete it when another inventory arrived, leaving its window
            # blocked for five minutes with no request left to retry.
            for old_key, expiry in list(self.pending.items()):
                if expiry <= now and old_key not in self.pending_sync_requests:
                    self.pending.pop(old_key, None)
            if key in self.pending_sync_requests or self.pending.get(key, 0) > now:
                return False
            if len(self.pending) >= MAX_PENDING_SYNC_REQUESTS:
                return False
            self.pending[key] = retry_at
            self.pending_sync_requests[key] = (dict(record), route, retry_at, 0)
        if self._send_routed_record(record, route):
            return True
        with self.pending_lock:
            self.pending.pop(key, None)
            self.pending_sync_requests.pop(key, None)
        return False

    def _retry_pending_sync_requests(self) -> None:
        """Retry unanswered history pages with exponential, bounded backoff."""
        now = time.monotonic()
        due: list[tuple[tuple[str, str, int], dict, tuple, int]] = []
        with self.pending_lock:
            for key in list(self.pending_sync_requests):
                if key not in self.pending:
                    self.pending_sync_requests.pop(key, None)
            for key, (record, route, retry_at, attempts) in list(
                    self.pending_sync_requests.items()):
                if retry_at > now:
                    continue
                if attempts >= MAX_SYNC_REQUEST_RETRIES:
                    self.pending_sync_requests.pop(key, None)
                    self.pending.pop(key, None)
                    origin = record.get("o", "")
                    window = (origin, int(record.get("f", record.get("a", 0))),
                              int(record.get("u", record.get("b", 0))))
                    if self.pending_sync_windows.get(window, (None,))[0] == record.get("to"):
                        self.pending_sync_windows.pop(window, None)
                    self.events.put(("status", "History page request timed out; use /sync to retry"))
                    continue
                next_attempt = attempts + 1
                retry_seconds = self._sync_retry_interval()
                delay = retry_seconds * (2 ** attempts)
                next_retry = now + delay * (1 + secrets.randbelow(1001) / 2000)
                self.pending_sync_requests[key] = (record, route, next_retry, next_attempt)
                self.pending[key] = next_retry
                due.append((key, record, route, next_attempt))
        for key, record, route, attempt in due:
            # A page-end may clear the request after the retry was collected
            # but before this loop sends it. Recheck so a completed page does
            # not cause one last duplicate batch on air.
            with self.pending_lock:
                current = self.pending_sync_requests.get(key)
                still_pending = (current is not None and current[0] == record and
                                 current[1] == route and current[3] == attempt)
            if still_pending:
                retry_record = dict(record)
                sparse_sequences = record.get("r", []) if "h" not in record else []
                sparse_has_data = any(
                    self.store.has_message(record["o"], int(seq))
                    for seq in sparse_sequences)
                range_has_data = (not record.get("r") and
                                  self.store.has_history_data(
                                      record["o"], int(record.get("a", 0)),
                                      int(record.get("b", 0))))
                if (not record.get("v") and (sparse_has_data or range_has_data)):
                    # The peer's previous page may have arrived while only its
                    # addressed end record was lost. Ask for the page manifest
                    # first, then selectively repair any missing fragments.
                    retry_record["j"] = True
                    if not sparse_sequences:
                        retry_record.pop("h", None)
                retry_record["k"] = attempt
                self._send_routed_record(retry_record, (*route, attempt))

    def _note_sync_progress(self, origin: str, seq: int) -> None:
        """Treat the page timer as a silence timeout while fragments arrive."""
        now = time.monotonic()
        seconds = self._sync_retry_interval()
        deadline = now + seconds * (1 + secrets.randbelow(1001) / 2000)
        with self.pending_lock:
            for key, (record, route, retry_at, attempts) in list(self.pending_sync_requests.items()):
                if (record.get("o") == origin and
                        int(record.get("a", 0)) < seq <= int(record.get("b", 0))):
                    retry_at = max(retry_at, deadline)
                    self.pending_sync_requests[key] = (record, route, retry_at, attempts)
                    self.pending[key] = retry_at

    @staticmethod
    def _bound_timed_map(mapping: dict, now: float, limit: int) -> None:
        """Prune expired (value,expiry) records and cap active mesh state."""
        for key, state in list(mapping.items()):
            if state[1] <= now:
                mapping.pop(key, None)
        while len(mapping) > limit:
            mapping.pop(min(mapping, key=lambda key: mapping[key][1]), None)

    def _remember_receipt_ack(self, key: tuple[str, int, str, str], now: float) -> bool:
        """Remember one relay receipt ack without allowing unbounded mesh state."""
        for old_key, expiry in list(self.seen_receipt_acks.items()):
            if expiry <= now:
                self.seen_receipt_acks.pop(old_key, None)
        known = self.seen_receipt_acks.get(key, 0) > now
        if not known and len(self.seen_receipt_acks) >= MAX_SEEN_RECEIPT_ACKS:
            oldest = min(self.seen_receipt_acks, key=self.seen_receipt_acks.get)
            self.seen_receipt_acks.pop(oldest, None)
        self.seen_receipt_acks[key] = now + 3600
        return not known

    def _expire_peer_sightings(self) -> None:
        """Keep station suggestions and the live list limited to recent peers."""
        cutoff = time.monotonic() - HEARD_PEER_TTL
        with self.peer_lock:
            expired = [peer for peer, seen in self.peer_last_seen.items()
                       if seen <= cutoff]
            for peer in expired:
                self.peer_last_seen.pop(peer, None)
                self.peer_names.pop(peer, None)
                self.peer_nicks.pop(peer, None)
            for callsign, seen in list(self.cq_frames.items()):
                if seen <= cutoff:
                    self.cq_frames.pop(callsign, None)

    def _send_routed_record(self, record: dict, key: tuple, *, priority: int = 0,
                            operator_initiated: bool = False) -> bool:
        self._route_once(key, int(record.get("ttl", 4)))
        return self.send_record(record, priority=priority,
                                operator_initiated=operator_initiated)

    def _relay_record(self, record: dict, key: tuple, *, priority: int = 0) -> None:
        ttl = int(record.get("ttl", 0))
        if self._route_once(key, ttl):
            forwarded = dict(record)
            forwarded["ttl"] = ttl - 1
            self.send_record(forwarded, priority=priority)

    def publish(self, body: str, image_path: str | None = None, *,
                thumbnail: tuple[bytes, int, int] | None = None,
                card_attachment: dict | None = None,
                channel: str | None = None, nick: str | None = None):
        channel = self.channel if channel is None else channel
        nick = self.nick if nick is None else nick
        body = body.strip()
        if len(body.encode("utf-8")) > MAX_MESSAGE_BYTES:
            raise ValueError(f"message exceeds {MAX_MESSAGE_BYTES} UTF-8 bytes")
        if not all(char.isprintable() or char in "\t\n" for char in body):
            raise ValueError("message contains terminal control characters")
        if (image_path is not None or thumbnail is not None) and card_attachment is not None:
            raise ValueError("a message may contain one image or card attachment")
        image_bytes = None
        if image_path:
            image_bytes, width, height = make_thumbnail(image_path)
        elif thumbnail:
            image_bytes, width, height = thumbnail
        else:
            width = height = 0
        if not body and image_bytes is None:
            return
        timestamp = int(time.time())
        insert_options = {"signer": self.args.callsign if self.auth.enabled else ""}
        if card_attachment is not None:
            insert_options["card_attachment"] = card_attachment
        message = self.store.insert_local(
            channel, nick, timestamp, body,
            (image_bytes, width, height) if image_bytes is not None else None,
            **insert_options)
        attachment = message.get("image") or message.get("attachment")
        seq = int(message["s"])
        self._debug_log("MESSAGE_LOCAL", {"o": self.store.node, "s": seq,
                                          "ts": timestamp})
        self.events.put(("message", message))
        delivery_queued = True
        for frame in message_parts(origin=self.store.node, seq=seq, channel=channel,
                                   nick=nick, timestamp=timestamp, text=body,
                                   sender=self._message_sender_id()):
            try:
                if hasattr(self.data, "send_operator_initiated"):
                    self.data.send_operator_initiated(frame)
                else:
                    self.data.send(frame)
            except (OSError, queue.Full, ConnectionError) as exc:
                self.events.put(("status", f"Could not queue message fragment: {exc}"))
                delivery_queued = False
                break
        if not delivery_queued:
            self.pending_local_delivery.set()
        else:
            # Advertise the latest bounds for a rotating set of known origins.
            # Peers request only the sequence range beyond their local cursor.
            since = self._history_since()
            inventory = self.store.inventory(since)
            start = seq % len(inventory) if inventory else 0
            summary = (inventory[start:] + inventory[:start])[:8]
            if summary:
                self.send_record({"t": "k", "from": self.store.node,
                                  "h": [list(item) for item in summary]})
        if attachment and attachment.get("kind", "image") == "image":
            self._send_attachment(message)
        if self.auth.enabled:
            try:
                self.events.put(("status", "Signing outgoing message with ARAP…"))
                self._queue_signature(dict(message))
            except RuntimeError as exc:
                self.events.put(("status", f"ARAP signing could not be queued: {exc}"))


    def queue_image(self, path: str, caption: str = "") -> None:
        """Prepare and publish a thumbnail without blocking the chat UI."""
        channel, nick = self.channel, self.nick
        if not self.image_slots.acquire(blocking=False):
            self.events.put(("status", "Thumbnail queue is busy; wait for an image to finish"))
            return
        self.events.put(("status", "Preparing radio thumbnail…"))
        try:
            self.image_executor.submit(self._prepare_and_publish_image, path, caption,
                                       channel, nick)
        except RuntimeError as exc:
            self.image_slots.release()
            self.events.put(("status", f"Image could not be queued: {exc}"))

    def _prepare_and_publish_image(self, path: str, caption: str,
                                   channel: str, nick: str) -> None:
        try:
            thumbnail = make_thumbnail(path)
            self.publish(caption, thumbnail=thumbnail, channel=channel, nick=nick)
        except (OSError, ValueError, OverflowError) as exc:
            self.events.put(("status", f"Image could not be attached: {exc}"))
        finally:
            self.image_slots.release()

    def queue_card(self, card: str | PixelCard) -> None:
        """Queue one PixelQSO card as a log reference with a deferred payload."""
        channel, nick = self.channel, self.nick
        if not self.image_slots.acquire(blocking=False):
            self.events.put(("status", "Media preparation is busy; wait for it to finish"))
            return
        self.events.put(("status", "Preparing PixelQSO card…"))
        try:
            self.image_executor.submit(self._prepare_and_publish_card, card, channel, nick)
        except RuntimeError as exc:
            self.image_slots.release()
            self.events.put(("status", f"PixelQSO card could not be queued: {exc}"))

    def _prepare_and_publish_card(self, card: str | PixelCard,
                                  channel: str, nick: str) -> None:
        try:
            selected = card if isinstance(card, PixelCard) else load_pixel_card(card)
            body, payload, reference = encode_pixel_card_reference(selected)
            self.publish(body, channel=channel, nick=nick,
                         card_attachment={"payload": payload, "width": selected.width,
                                          "height": selected.height,
                                          "digest": reference.digest, "metadata": body})
        except (OSError, ValueError, OverflowError) as exc:
            self.events.put(("status", f"PixelQSO card could not be sent: {exc}"))
        finally:
            self.image_slots.release()

    def _on_frame(self, port: int, payload: bytes, reception: dict | None = None):
        if port != self.data.port:
            return
        record = unpack(payload)
        if not record:
            self._debug_log("RX_INVALID", f"port={port} bytes={len(payload)}")
            return
        self._debug_log("RX", record)
        kind = record.get("t")
        if reception and reception.get("source"):
            sender = (record.get("from") if kind == "k" else
                      record.get("p") if kind == "i" and record.get("ttl") == 4 else None)
            if isinstance(sender, str) and NODE_ID.fullmatch(sender):
                if self.store.note_origin_station(sender, reception["source"]):
                    self.events.put(("reception", None))
        if kind == "k":
            peer = str(record.get("from", ""))
            if peer and peer != self.store.node:
                entries = [(str(origin), int(low), int(high))
                           for origin, low, high in record.get("h", [])]
                self._handle_inventory_entries(peer, entries, ttl=4)
            return
        if kind in {"B", "C"}:
            self._handle_auth_challenge_record(kind, record)
            return
        if kind == "A":
            origin, seq, reference = (record.get("o"), record.get("s"), record.get("r"))
            if (not isinstance(origin, str) or not NODE_ID.fullmatch(origin) or
                    type(seq) is not int or seq < 1 or
                    not isinstance(reference, str) or
                    not re.fullmatch(r"AR1[A-Z2-7]{16}", reference)):
                return
            ttl = int(record.get("ttl", 4))
            route_key = ("a", origin, seq, reference)
            if self._route_once(route_key, ttl):
                self.send_record({"t": "A", "o": origin, "s": seq,
                                  "r": reference, "ttl": ttl - 1}, priority=10)
            self.store.set_auth_reference(origin, seq, reference)
            auth = self.store.auth_for(origin, seq)
            self.events.put(("auth", {"o": origin, "s": seq,
                                       "status": auth["status"],
                                       "signer": auth["signer"],
                                       "detail": auth["detail"],
                                       "reference": reference}))
            if self.store.has_message(origin, seq):
                self._queue_reference_resolution(origin, seq, reference)
            return
        if kind == "p":
            ttl = int(record.get("ttl", 4))
            route_key = ("p", record["o"], int(record["s"]), int(record["i"]))
            if self._route_once(route_key, ttl):
                try:
                    self.data.send(relay_proof_part(record, ttl - 1), priority=10)
                except (OSError, ValueError, queue.Full, ConnectionError) as exc:
                    self.events.put(("status", f"Could not relay ARAP proof: {exc}"))
            proof = self.store.add_proof_fragment(record)
            if proof is not None:
                self._verify_stored_proof(record["o"], int(record["s"]), proof)
            return
        if kind == "x":
            chunk_key = (record.get("id", ""), record["to"], record["o"],
                         int(record["s"]), record["h"], int(record["i"]))
            now = time.monotonic()
            if record.get("ttl", 0) > 0 and record["to"] != self.store.node:
                ttl = int(record["ttl"])
                with self.image_lock:
                    previous_ttl, expiry = self.seen_image_chunks.get(chunk_key, (-1, 0))
                    forward = expiry <= now or ttl > previous_ttl
                    if forward:
                        self.seen_image_chunks[chunk_key] = (ttl, now + 600)
                        self._bound_timed_map(self.seen_image_chunks, now,
                                              MAX_IMAGE_CHUNKS_SEEN)
                if forward:
                    self._forward_image_part(record)
            self._receive_image_part(record)
            return
        if kind == "d":
            recipient = record.get("to", "")
            if recipient and recipient != self.store.node:
                ttl = int(record.get("ttl", 4))
                key = ("d", recipient, record["o"], int(record["s"]), int(record["i"]))
                if self._route_once(key, ttl):
                    self._queue_message_relay(key, record, ttl)
                else:
                    self._suppress_duplicate_message_relay(key, ttl)
                return
            source = (reception or {}).get("source")
            origin = record.get("o", "")
            if (not recipient and source and not (reception or {}).get("repeated") and
                    isinstance(origin, str) and NODE_ID.fullmatch(origin)):
                # Unaddressed message fragments are sent by their origin. Learn
                # the radio-source mapping here instead of waiting for a separate
                # inventory, which may be lost or arrive after the message.
                if self.store.note_origin_station(origin, source):
                    self.events.put(("reception", None))
            self._note_sync_progress(record["o"], int(record["s"]))
            fragment_reception = reception or {}
            sender_id = record.get("from")
            if (not fragment_reception.get("source") and
                    isinstance(sender_id, str) and NODE_ID.fullmatch(sender_id)):
                fragment_reception = {"source": sender_id, "repeated": False}
            result = self.store.add_fragment(record, fragment_reception)
            if result:
                message, inserted = result
                if inserted:
                    self._debug_log("MESSAGE_STORED", {"o": message["o"], "s": message["s"],
                                                       "ts": message["ts"],
                                                       "reception": fragment_reception})
                    existing_proof = self.store.proof_for(message["o"], message["s"])
                    if existing_proof:
                        self._verify_stored_proof(message["o"], message["s"], existing_proof)
                    else:
                        auth_record = self.store.auth_for(message["o"], message["s"])
                        if auth_record.get("reference"):
                            self._queue_reference_resolution(
                                message["o"], int(message["s"]), auth_record["reference"])
                    mention_targets = self._mentioned_here(message["b"])
                    if message["c"] != self.channel:
                        self.store.mark_unread(message["o"], int(message["s"]), message["c"],
                                               mention=bool(mention_targets))
                    self.events.put(("message", message))
                    # Tell neighboring stations this relay now has the origin.
                    # Rate limiting coalesces batches of synced history.
                    self._queue_relay_announcement(message["o"])
                    for target in mention_targets:
                        self._create_receipt(message, target)
                if message["o"] != self.store.node:
                    try:
                        card_reference = decode_pixel_card_reference(message["b"])
                    except ValueError as exc:
                        card_reference = None
                        self._debug_log("INVALID_PIXELQSO_CARD_REFERENCE", str(exc))
                    else:
                        if card_reference is not None:
                            before = self.store.attachment_for(message["o"], int(message["s"]))
                            valid = self.store.set_attachment(
                                message["o"], int(message["s"]), card_reference.digest,
                                card_reference.size, card_reference.width,
                                card_reference.height, kind="card", metadata=message["b"])
                            if not valid:
                                self._debug_log("CARD_REFERENCE_ATTACHMENT_MISMATCH",
                                                {"o": message["o"], "s": message["s"]})
                            elif before is None and not inserted:
                                self.events.put(("attachment", {"o": message["o"],
                                                                  "s": message["s"]}))
                            cached_card = self.store.attachment_blob_for(card_reference.digest)
                            if valid and cached_card is not None:
                                self._save_received_card_bytes(
                                    cached_card, card_reference, message["o"],
                                    int(message["s"]))
                        else:
                            try:
                                received_card = decode_pixel_card_message(message["b"])
                            except ValueError as exc:
                                self._debug_log("INVALID_PIXELQSO_CARD", str(exc))
                            else:
                                if received_card is not None:
                                    try:
                                        save_received_pixel_card_message(
                                            received_card, origin=message["o"],
                                            seq=int(message["s"]))
                                    except (OSError, ValueError) as exc:
                                        self.events.put(("status", f"Could not save PixelQSO received card: {exc}"))
        elif kind == "a":
            origin, seq = record.get("o", ""), int(record.get("s", 0))
            digest = record.get("h", "")
            recipient = record.get("to", "")
            before = self.store.attachment_for(origin, seq)
            attachment_kind = record.get("k", "image")
            if self.store.set_attachment(
                    origin, seq, digest, int(record.get("b", 0)),
                    int(record.get("w", 0)), int(record.get("hgt", 0)),
                    kind=attachment_kind, metadata=record.get("m", "")):
                rows = self.store.messages_between(origin, seq - 1, seq)
                if rows and before is None:
                    key = "image" if attachment_kind == "image" else "attachment"
                    rows[0][key] = {"digest": digest, "size": int(record["b"]),
                                    "width": int(record["w"]), "height": int(record["hgt"]),
                                    "kind": attachment_kind,
                                    "metadata": record.get("m", "")}
                    existing = self.store.proof_for(origin, seq)
                    if existing:
                        self._verify_stored_proof(origin, seq, existing)
                    self.events.put(("attachment", {"o": origin, "s": seq}))
                    self._queue_relay_announcement(origin)
            ttl = int(record.get("ttl", 4))
            route_key = ("a", recipient, origin, seq, digest)
            if (not recipient or recipient != self.store.node) and self._route_once(route_key, ttl):
                forwarded = dict(record)
                forwarded["ttl"] = ttl - 1
                self.send_record(forwarded)
        elif kind == "g":
            requester = record.get("p", "")
            raw_request_id = record.get("id")
            request_id = "legacy" if raw_request_id is None else str(raw_request_id)
            origin, seq, digest = record.get("o", ""), int(record.get("s", 0)), record.get("h", "")
            if (not isinstance(requester, str) or not NODE_ID.fullmatch(requester) or
                    not isinstance(origin, str) or not NODE_ID.fullmatch(origin) or
                    not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest) or
                    (request_id != "legacy" and
                     not re.fullmatch(r"[0-9a-fA-F]{8}", request_id))):
                return
            request_key = (requester, origin, seq, digest, request_id)
            now = time.monotonic()
            ttl = int(record.get("ttl", 0))
            with self.image_lock:
                previous_ttl, expiry = self.seen_image_requests.get(request_key, (-1, 0))
                if expiry > now and ttl <= previous_ttl:
                    return
                self.seen_image_requests[request_key] = (ttl, now + 120)
                self._bound_timed_map(self.seen_image_requests, now, MAX_IMAGE_REQUESTS)
                self._bound_timed_map(self.image_claims, now, MAX_IMAGE_REQUESTS)
            attachment = self.store.attachment_for(origin, seq)
            if attachment and attachment["digest"] == record.get("h"):
                body = self.store.attachment_blob_for(attachment["digest"])
                if body:
                    requested = record.get("i")
                    indexes = None
                    if requested is not None:
                        if (not isinstance(requested, list) or not requested or
                                len(requested) > 16 or
                                any(type(index) is not int for index in requested)):
                            return
                        indexes = sorted(set(requested))
                        fragment_count = max(1, (len(body) + 138) // 139)
                        if any(index < 0 or index >= fragment_count for index in indexes):
                            return
                    self._schedule_image_response(request_key, requester, attachment, body,
                                                  indexes)
                    # A complete cache can satisfy this request. Flooding it
                    # farther only recruits more responders and echoes it back.
                    return
            if ttl > 0:
                record["ttl"] = ttl - 1
                self.send_record(record)
        elif kind == "y":
            requester, origin = record.get("p", ""), record.get("o", "")
            seq, digest, request_id = int(record.get("s", 0)), record.get("h", ""), str(record.get("id", ""))
            claimant = record.get("by", "")
            request_key = (requester, origin, seq, digest, request_id)
            if (not isinstance(requester, str) or not NODE_ID.fullmatch(requester) or
                    not isinstance(origin, str) or not NODE_ID.fullmatch(origin) or
                    not isinstance(claimant, str) or not NODE_ID.fullmatch(claimant) or
                    not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
                return
            with self.image_lock:
                current = self.image_claims.get(request_key)
                now = time.monotonic()
                # If claims cross on different mesh paths, converge on the
                # lexicographically lowest station ID. A candidate with a
                # pending settle timer can then stand down before sending.
                if (current and current[1] > now and
                        claimant >= current[0]):
                    return
                self.image_claims[request_key] = (claimant, now + 120)
                download_key = (origin, seq, digest)
                if requester == self.store.node and download_key in self.pending_images:
                    attempt, _, expires = self.pending_images[download_key]
                    self.pending_images[download_key] = (attempt, now + self._image_retry_interval(),
                                                         expires)
                self._bound_timed_map(self.image_claims, now, MAX_IMAGE_REQUESTS)
                timer = self.image_response_timers.get(request_key)
                if timer and claimant != self.store.node:
                    timer.cancel()
                    self.image_response_timers.pop(request_key, None)
            ttl = int(record.get("ttl", 0))
            if ttl > 0 and requester != self.store.node:
                record["ttl"] = ttl - 1
                self.send_record(record)
        elif kind == "i":
            peer = record.get("p", "")
            if not isinstance(peer, str) or not NODE_ID.fullmatch(peer) or peer == self.store.node:
                return
            name = str(record.get("n", peer[:8])).upper()
            if (not 1 <= len(name) <= 10 or
                    any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/-" for char in name)):
                name = peer[:8]
            with self.peer_lock:
                now = time.monotonic()
                if peer not in self.peer_last_seen and len(self.peer_last_seen) >= MAX_HEARD_PEERS:
                    oldest = min(self.peer_last_seen, key=self.peer_last_seen.get)
                    self.peer_last_seen.pop(oldest, None)
                    self.peer_names.pop(oldest, None)
                    self.peer_nicks.pop(oldest, None)
                self.peer_last_seen[peer] = now
                self.peer_names[peer] = name
                nick = str(record.get("nick", name))
                if (not nick or len(nick.encode("utf-8")) > 16 or
                        not all(char.isprintable() for char in nick)):
                    nick = name
                self.peer_nicks[peer] = nick
            entries = record.get("v")
            try:
                inventory_ttl = int(record.get("ttl", 4))
            except (TypeError, ValueError):
                inventory_ttl = 0
            if isinstance(entries, list):
                lows = record.get("l")
                origins = []
                for index, item in enumerate(entries):
                    if not isinstance(item, list) or len(item) not in (2, 3):
                        continue
                    if not isinstance(item[0], str) or not NODE_ID.fullmatch(item[0]):
                        continue
                    if len(item) == 3:  # accept early sparse-inventory builds
                        low, high = item[1], item[2]
                    else:
                        low = (lows[index] if isinstance(lows, list) and
                               index < len(lows) and type(lows[index]) is int else 1)
                        high = item[1]
                    if (type(low) is int and type(high) is int and
                            1 <= low <= high <= 0xFFFFFFFF):
                        origins.append((item[0], low, high))
            else:
                origin = record.get("o", "")
                if not isinstance(origin, str) or not NODE_ID.fullmatch(origin):
                    return
                high = int(record.get("m", 0))
                low = int(record.get("a", 1))
                if not 1 <= low <= high <= 0xFFFFFFFF:
                    return
                origins = [(origin, low, high)]
            self._handle_inventory_entries(peer, origins, ttl=inventory_ttl)
            # A cache announces its own inventory after storing new data.
            # Flooded copies cannot serve a one-hop broadcast history page.
        elif kind == "q":
            requester = record.get("from", "")
            origin = record.get("o", "")
            after = int(record.get("a", 0))
            through = int(record.get("b", 0))
            now_epoch = int(time.time())
            try:
                requested_since = int(record.get("d", self._history_since(now_epoch)))
            except (TypeError, ValueError):
                requested_since = self._history_since(now_epoch)
            since = min(now_epoch, max(requested_since, self._history_since(now_epoch)))
            proof_page = bool(record.get("v"))
            target = record.get("to", "")
            repair_sequences = record.get("r", [])
            repair_fragments = record.get("h", [])
            request_attempt = record.get("k", 0)
            repair_limit = (SYNC_PAGE_MESSAGES if "h" in record
                            else SYNC_SPARSE_MESSAGES)
            if (not isinstance(repair_sequences, list) or len(repair_sequences) >
                    repair_limit or any(type(seq) is not int or seq <= 0
                                              for seq in repair_sequences) or
                    len(set(repair_sequences)) != len(repair_sequences) or
                    type(request_attempt) is not int or not 0 <= request_attempt <= 31):
                return
            repair_sequences = sorted(repair_sequences)
            fragment_masks: dict[int, int] = {}
            if "h" in record:
                if (not isinstance(repair_fragments, list) or
                        len(repair_fragments) != len(repair_sequences) or
                        any(not isinstance(item, list) or len(item) != 2 or
                            type(item[0]) is not int or type(item[1]) is not int or
                            item[0] not in repair_sequences or
                            not 0 <= item[1] < (1 << MAX_PARTS)
                            for item in repair_fragments) or
                        len({item[0] for item in repair_fragments}) != len(repair_fragments)):
                    return
                fragment_masks = {seq: mask for seq, mask in repair_fragments}
            if not all(isinstance(value, str) and NODE_ID.fullmatch(value)
                       for value in (requester, origin, target)):
                return
            if repair_sequences and (proof_page or
                                     any(seq <= after or seq > through
                                         for seq in repair_sequences)):
                return
            route_key = ("q", requester, target, origin, after, through,
                         proof_page, bool(record.get("j")), request_attempt,
                         tuple((seq, fragment_masks[seq]) for seq in repair_sequences)
                         if fragment_masks else tuple(repair_sequences))
            if repair_sequences:
                self._allow_message_repair(requester, origin, repair_sequences)
            if target != self.store.node:
                record.setdefault("ttl", 4)
                self._relay_record(record, route_key)
                return
            if len(requester) != 32 or len(origin) != 32 or through < after:
                return
            if not self._claim_sync_response(route_key, time.monotonic()):
                return
            if repair_sequences:
                rows = []
                for seq in repair_sequences:
                    row = self.store.messages_between(origin, seq - 1, seq,
                                                      limit=1, since=since)
                    if row:
                        rows.append(row[0])
            else:
                rows = self.store.messages_between(
                    origin, after, through,
                    limit=20 if proof_page else SYNC_PAGE_MESSAGES,
                    since=since)
            response_priority = 10
            for row in rows:
                if not record.get("v") and not record.get("j"):
                    # History pages are public gossip: every listener can
                    # cache a complete message and advertise the origin later.
                    # The page-end remains addressed to the requester, so
                    # overhearers do not echo the just-heard fragments.
                    frames = message_parts(origin=row["o"], seq=row["s"], channel=row["c"],
                                           nick=row["n"], timestamp=row["ts"], text=row["b"],
                                           sender=self._message_sender_id())
                    if (len(rows) == 1 and len(frames) == 1 and
                            0 <= now_epoch - int(row["ts"]) <= LIVE_MESSAGE_REPAIR_SECONDS):
                        response_priority = 0
                    mask = fragment_masks.get(int(row["s"]))
                    selected = ([(index, frame) for index, frame in enumerate(frames)
                                 if mask == 0 or mask & (1 << index)]
                                if mask is not None else list(enumerate(frames)))
                    for _, frame in selected:
                        try:
                            self.data.send(frame, priority=response_priority)
                        except (OSError, queue.Full, ConnectionError):
                            break
                # Attachment metadata is tiny and broadcast so every cache
                # can serve the announced thumbnail later. Repeat it on proof
                # repair pages too; a lost metadata frame must not orphan an image.
                attachment = (None if record.get("j") else
                              self.store.attachment_for(row["o"], row["s"]))
                if attachment and attachment.get("kind", "image") == "image":
                    attachment_record = {"t": "a", "o": attachment["origin"],
                                         "s": attachment["seq"], "h": attachment["digest"],
                                         "b": attachment["size"], "w": attachment["width"],
                                         "hgt": attachment["height"], "ttl": 4}
                    self._send_routed_record(
                        attachment_record,
                        ("a", "", attachment["origin"], attachment["seq"],
                         attachment["digest"]), priority=10)
                auth = self.store.auth_for(row["o"], row["s"])
                if auth.get("reference"):
                    self.send_record({"t": "A", "o": row["o"], "s": row["s"],
                                      "r": auth["reference"], "ttl": 4}, priority=10)
            page_last = (int(record.get("b", after)) if repair_sequences else
                         int(rows[-1]["s"]) if rows else after)
            end_record = {"t": "e", "to": requester, "from": self.store.node,
                          "o": origin, "a": after,
                          # b is the last returned sequence, not the scan
                          # ceiling. This keeps empty gaps from causing pages.
                          "b": page_last,
                          "m": self.store.highwater(origin, since), "d": since,
                          "u": int(record.get("u", through)),
                          "f": int(record.get("f", after)),
                          "x": (bool(record.get("x")) if repair_sequences else
                                len(rows) == SYNC_PAGE_MESSAGES),
                          "g": (repair_sequences if repair_sequences else
                                [] if proof_page else [int(row["s"]) for row in rows]),
                          "j": bool(repair_sequences),
                          "v": proof_page, "k": request_attempt,
                          "ttl": 0 if int(record.get("ttl", 4)) == 0 else 4}
            self._send_routed_record(end_record,
                                     ("e", self.store.node, requester, origin, after,
                                      end_record["b"], end_record["f"], proof_page,
                                      tuple(end_record["g"]), end_record["j"],
                                      request_attempt),
                                     priority=response_priority)
        elif kind == "e":
            page_attempt = record.get("k", 0)
            if type(page_attempt) is not int or not 0 <= page_attempt <= 31:
                return
            end_key = ("e", record.get("from", ""), record.get("to", ""),
                       record.get("o", ""), int(record.get("a", 0)),
                       int(record.get("b", 0)), int(record.get("f", 0)),
                       bool(record.get("v")),
                       tuple(record.get("g", [])) if isinstance(record.get("g", []), list)
                       else (),
                       bool(record.get("j")), page_attempt)
            if record.get("to") != self.store.node:
                record.setdefault("ttl", 4)
                ttl = int(record.get("ttl", 0))
                if self._route_once(end_key, ttl):
                    self._queue_end_relay(end_key, record, ttl)
                return
            self._route_once(end_key, int(record.get("ttl", 0)))
            origin = record.get("o", "")
            after = int(record.get("a", 0))
            # New peers carry the original inventory ceiling in `u`; `m` is
            # the responder's high-water mark and may be higher than the
            # advertised window. Keep paging bounded to the requested window.
            through = int(record.get("u", record.get("m", record.get("b", 0))))
            try:
                since = int(record.get("d", self._history_since()))
            except (TypeError, ValueError):
                since = self._history_since()
            last_returned = int(record.get("b", 0))
            window_ceiling = int(record.get("u", through))
            window_start = int(record.get("f", after))
            responder = record.get("from", "")
            if record.get("v"):
                # Keep the request key until its timeout to dampen duplicate
                # proof-only exchanges from several inventory relays.
                return
            with self.pending_lock:
                # End frames acknowledge one page request. Match its cursor as
                # well as peer and origin so a delayed response cannot clear
                # another in-flight page for the same history stream.
                candidates = [item for item in self.pending
                              if item[0] == responder and item[1] == origin
                              and item[2] == after]
                for item in candidates:
                    self.pending.pop(item, None)
                    self.pending_sync_requests.pop(item, None)
            for item in candidates:
                peer, _, after = item
                page_sequences = record.get("g", [])
                page_sequence_limit = (SYNC_SPARSE_MESSAGES if record.get("j")
                                       else SYNC_PAGE_MESSAGES)
                page_receipt_valid = (
                    "g" in record and isinstance(page_sequences, list) and
                    len(page_sequences) <= page_sequence_limit and
                    all(type(seq) is int and after < seq <= last_returned
                        for seq in page_sequences))
                if not page_receipt_valid:
                    page_sequences = []
                missing = ([seq for seq in page_sequences
                            if not self.store.has_message(origin, seq)]
                           if page_receipt_valid else [])
                repair_key = (peer, origin, after)
                if missing:
                    attempts = self.sync_repair_attempts.get(repair_key, 0)
                    if attempts < SYNC_PAGE_REPAIR_LIMIT:
                        self.sync_repair_attempts[repair_key] = attempts + 1
                        if len(self.sync_repair_attempts) > MAX_PENDING_SYNC_REQUESTS:
                            self.sync_repair_attempts.pop(
                                next(iter(self.sync_repair_attempts)), None)
                        repair_request = {
                            "t": "q", "to": peer, "from": self.store.node,
                            "o": origin, "a": after, "b": last_returned,
                            "d": since, "f": window_start, "u": window_ceiling,
                            "x": bool(record.get("x")), "r": missing,
                            "ttl": 0,
                        }
                        if len(missing) <= SYNC_PAGE_MESSAGES:
                            repair_request["h"] = [
                                [seq, self.store.missing_fragment_mask(origin, seq)]
                                for seq in missing]
                        repair_route = ("q", self.store.node, peer, origin,
                                        after, last_returned, False, False, 0,
                                        tuple(missing))
                        self._send_sync_request(repair_request, repair_key,
                                                repair_route)
                        continue
                else:
                    self.sync_repair_attempts.pop(repair_key, None)
                local = self.store.contiguous(origin)
                if local > after:
                    # A completed history page is now available to other peers.
                    self._queue_relay_announcement(origin)
                # New peers page by actual stored rows, skipping arbitrary
                # sequence gaps. The fallback supports older peers whose end
                # record only reports a contiguous high-water value.
                has_more = bool(record.get("x"))
                if ("x" in record and not has_more and not missing and
                        (page_receipt_valid or "g" not in record)):
                    window_key = (origin, window_start, window_ceiling)
                    now = time.monotonic()
                    with self.pending_lock:
                        self.completed_sync_windows = {
                            key: expiry for key, expiry in self.completed_sync_windows.items()
                            if expiry > now
                        }
                        if len(self.completed_sync_windows) >= MAX_COMPLETED_SYNC_WINDOWS:
                            oldest = min(self.completed_sync_windows,
                                         key=self.completed_sync_windows.get)
                            self.completed_sync_windows.pop(oldest, None)
                        if page_sequences or self.store.contiguous(origin) >= window_ceiling:
                            self.completed_sync_windows[window_key] = (
                                now + SYNC_WINDOW_RETRY_SECONDS)
                        if self.pending_sync_windows.get(window_key, (None,))[0] == responder:
                            self.pending_sync_windows.pop(window_key, None)
                if (has_more and last_returned > after) or (
                        "x" not in record and after < through):
                    next_after = (last_returned if has_more else
                                  min(through, max(after + SYNC_PAGE_MESSAGES, local)))
                    key = (peer, origin, next_after)
                    window_ceiling = int(record.get("u", through))
                    if page_receipt_valid:
                        remaining_count = self.store.missing_sequence_count(
                            origin, next_after, window_ceiling)
                        if remaining_count:
                            sequences = self.store.missing_sequences(
                                origin, next_after, window_ceiling,
                                limit=SYNC_SPARSE_MESSAGES)
                            if sequences:
                                next_through = sequences[-1]
                                request_record = {
                                    "t": "q", "to": peer, "from": self.store.node,
                                    "o": origin, "a": next_after, "b": next_through,
                                    "d": since, "f": window_start, "u": window_ceiling,
                                    "x": len(sequences) < remaining_count,
                                    "r": sequences, "ttl": 0,
                                }
                                self._send_sync_request(
                                    request_record, key,
                                    ("q", self.store.node, peer, origin, next_after,
                                     next_through, False, False, 0, tuple(sequences)))
                    else:
                        next_through = through if "x" in record else min(
                            through, next_after + SYNC_PAGE_MESSAGES)
                        request_record = {"t": "q", "to": peer, "from": self.store.node,
                                          "o": origin, "a": next_after,
                                          "b": next_through, "d": since,
                                          "f": window_start, "u": window_ceiling,
                                          "ttl": 0}
                        self._send_sync_request(
                            request_record, key,
                            ("q", self.store.node, peer, origin, next_after,
                             next_through, False, ()))
        elif kind == "w":
            requester = record.get("p", "")
            if requester and requester != self.store.node:
                record.setdefault("ttl", 4)
                ttl = int(record["ttl"])
                route_key = ("w", requester, record.get("v", 1))
                if self._route_once(route_key, max(ttl, 1)):
                    self.announce(legacy=record.get("v") != 2)
        elif kind == "r":
            origin, seq = record.get("o", ""), int(record.get("s", 0))
            recipient_id = record.get("p") or record.get("from", "")
            recipient = str(record.get("n", ""))
            mention = str(record.get("m", ""))
            received_at = int(record.get("at", time.time()))
            inserted = self.store.add_relay_receipt(origin, seq, recipient_id, recipient,
                                                    mention, received_at)
            if not inserted and not self.store.has_relay_receipt(
                    origin, seq, recipient_id, mention):
                return
            if origin == self.store.node:
                if inserted and self.store.mark_receipt(origin, seq, recipient,
                                                        mention, received_at):
                    self.events.put(("receipt", {"s": seq, "n": recipient, "m": mention}))
                self.store.mark_relay_receipt_delivered(origin, seq, recipient_id, mention)
                # Receipts are retried until their acknowledgement arrives.
                # Re-send the ACK for a duplicate receipt so one lost ACK does
                # not leave the sender waiting forever.
                self.send_record({"t": "ra", "o": origin, "s": seq,
                                  "p": recipient_id, "m": mention, "ttl": 16})
            else:
                record["p"] = recipient_id
                record["from"] = self.store.node
                self._forward_pending_receipts()
        elif kind == "ra":
            origin, seq, recipient_id = (record.get("o", ""), int(record.get("s", 0)),
                                         record.get("p", ""))
            mention = str(record.get("m", ""))
            ack_key = (origin, seq, recipient_id, mention)
            self.store.mark_relay_receipt_delivered(
                origin, seq, recipient_id, mention if mention else None)
            now = time.monotonic()
            if self._remember_receipt_ack(ack_key, now):
                ttl = int(record.get("ttl", 0))
                if ttl > 0:
                    record["ttl"] = ttl - 1
                    self.send_record(record)

    def _handle_inventory_entry(self, peer: str, origin: str, low: int,
                                high: int, *, ttl: int = 4) -> None:
        self._handle_inventory_entries(peer, [(origin, low, high)], ttl=ttl)

    def _handle_inventory_entries(self, peer: str, entries, *, ttl: int = 4) -> None:
        if ttl < 4:
            return
        entries = list(entries)
        now = time.monotonic()
        with self.pending_lock:
            self.completed_sync_windows = {
                key: expiry for key, expiry in self.completed_sync_windows.items()
                if expiry > now
            }
            self.pending_sync_windows = {
                key: state for key, state in self.pending_sync_windows.items()
                if state[1] > now
            }
            completed = set(self.completed_sync_windows)
            active = {(key[0], key[1]) for key in self.pending_sync_windows}

        # Split the remaining global budget across the advertised origins so
        # the first stream cannot consume every history slot in the exchange.
        candidates = []
        for index, (origin, low, high) in enumerate(entries):
            after = max(self.store.contiguous(origin), low - 1)
            window = (origin, low - 1, high)
            missing = self.store.missing_sequence_count(origin, after, high)
            if missing and window not in completed and (origin, low - 1) not in active:
                candidates.append((index, missing))

        limits = {}
        with self.pending_lock:
            remaining = self.sync_history_budget_remaining
        if remaining is None:
            limits = {index: None for index, _ in candidates}
        else:
            candidates_left = len(candidates)
            for index, requested in candidates:
                quota = max(1, remaining // candidates_left) if candidates_left else 0
                amount = min(requested, quota, remaining)
                limits[index] = amount
                remaining -= amount
                candidates_left -= 1

        for index, (origin, low, high) in enumerate(entries):
            after = max(self.store.contiguous(origin), low - 1)
            has_missing = self.store.missing_sequence_count(origin, after, high) > 0
            request_limit = limits.get(index, 0 if has_missing else None)
            self._handle_inventory_entry_limited(
                peer, origin, low, high, ttl=ttl, request_limit=request_limit)

    def _handle_inventory_entry_limited(self, peer: str, origin: str, low: int,
                                        high: int, *, ttl: int = 4,
                                        request_limit: int | None = None) -> None:
        # A forwarded inventory is useful gossip, but its owner may be outside
        # radio range. Ask a station that directly announced a cached copy so
        # the broadcast page can reach this node in one hop.
        if ttl < 4:
            return
        local = self.store.contiguous(origin)
        since = self._history_since()
        local_recent = self.store.highwater(origin, since)
        after = max(local, low - 1)
        window_start = low - 1
        window_key = (origin, window_start, high)
        now = time.monotonic()
        with self.pending_lock:
            self.completed_sync_windows = {
                key: expiry for key, expiry in self.completed_sync_windows.items()
                if expiry > now
            }
            already_synced = self.completed_sync_windows.get(window_key, 0) > now
            self.pending_sync_windows = {
                key: state for key, state in self.pending_sync_windows.items()
                if state[1] > now
            }
            active_window = any(key[0] == origin and key[1] == window_start
                                for key in self.pending_sync_windows)
        if already_synced or active_window:
            return
        missing_count = self.store.missing_sequence_count(origin, after, high)
        if missing_count:
            request = (peer, origin, after)
            with self.pending_lock:
                # Only one neighbor should serve a given origin window. This
                # prevents a relay and the origin from both sending the same
                # batch when their inventories arrive close together.
                if any(key[0] == origin and key[1] == window_start
                       for key in self.pending_sync_windows):
                    return
                remaining = self.sync_history_budget_remaining
                amount = missing_count if remaining is None else min(missing_count, remaining)
                if request_limit is not None:
                    amount = min(amount, request_limit)
                if amount <= 0:
                    return
                through = (high if amount == missing_count else
                           self.store.sequence_at_missing_offset(
                               origin, after, high, amount))
                if through is None:
                    return
                sequences = self.store.missing_sequences(
                    origin, after, through, limit=min(amount, SYNC_SPARSE_MESSAGES))
                if not sequences:
                    return
                request_window = (origin, window_start, through)
                if request_window in self.pending_sync_windows:
                    return
                if remaining is not None:
                    self.sync_history_budget_remaining = remaining - amount
                budget_generation = self.sync_history_budget_generation
                self.pending_sync_windows[request_window] = (
                    peer, now + SYNC_WINDOW_RETRY_SECONDS)
            batch_through = sequences[-1]
            request_record = {"t": "q", "to": peer, "from": self.store.node,
                              "o": origin, "a": after, "b": batch_through,
                              "d": since, "f": window_start, "u": through,
                              "x": len(sequences) < amount, "r": sequences,
                              "ttl": 0}
            if not self._send_sync_request(
                request_record, request,
                ("q", self.store.node, peer, origin, after, batch_through,
                 False, False, 0, tuple(sequences))):
                with self.pending_lock:
                    if self.pending_sync_windows.get(request_window, (None,))[0] == peer:
                        self.pending_sync_windows.pop(request_window, None)
                    if (self.sync_history_budget_generation == budget_generation and
                            self.sync_history_budget_remaining is not None):
                        self.sync_history_budget_remaining = min(
                            self._sync_exchange_budget(),
                            self.sync_history_budget_remaining + amount)
        elif local_recent > high:
            # Tell the mesh this station can serve history the advertising
            # peer lacks; do not wait for the next periodic inventory round.
            self._queue_relay_announcement(origin)
        # A missing detached proof leaves this message unchecked. We do not
        # request proof-only pages automatically; complete proofs received
        # with message history are still verified normally.

    def command(self, line: str):
        parts = line.strip().split(maxsplit=1)
        command = parts[0].lower() if parts else ""
        argument = parts[1].strip() if len(parts) == 2 else ""
        if command == "/quit":
            return False
        if command == "/join":
            if not CHANNEL.fullmatch(argument):
                self.events.put(("status", "Usage: /join #channel"))
            else:
                self.channel = argument
                self.store.save_channel(argument)
        elif command == "/nick":
            if (not argument or len(argument.encode("utf-8")) > 16 or
                    not all(char.isprintable() for char in argument)):
                self.events.put(("status", "Nick must be printable and 1–16 UTF-8 bytes"))
            else:
                self.nick = argument
                self.store.save_nick(self.nick)
                self.announce()
                self.events.put(("status", f"Nick is now {self.nick}"))
        elif command == "/history":
            self.events.put(("status", "Use /history in the chat interface to show recent messages"))
        elif command == "/auth":
            if not argument or len(argument.split()) != 1:
                self.events.put(("status", "Usage: /auth CALLSIGN"))
            else:
                try:
                    self.auth_executor.submit(self._issue_auth_challenge, argument)
                except RuntimeError:
                    self.events.put(("status", "ARAP is busy; try /auth again shortly"))
        elif command == "/channels":
            names = sorted(set(self.store.channels()) | {self.channel})
            self.events.put(("status", "Channels: " + " ".join(names)))
        elif command == "/sync":
            # Make an explicit sync a user-controlled retry for history pages.
            self.sync(retry=True)
            if self.args.transport == "connected" and not self.data.session_event.is_set():
                self.events.put(("status", "Connect to a station before syncing history"))
            else:
                self.events.put(("status", "History sync requested"))
        elif command == "/connect":
            if not argument:
                self.events.put(("status", "Usage: /connect CALLSIGN"))
            else:
                try:
                    self.data.connect_peer(argument)
                    self.events.put(("status", f"Calling {argument.upper()} over Data2G ARQ"))
                except (ConnectionError, RuntimeError, ValueError, OSError) as exc:
                    self.events.put(("status", f"Could not connect: {exc}"))
        elif command == "/cq":
            try:
                self.data.send_cqframe()
                self.events.put(("status", "CQ frame sent; listening for connected stations"))
            except (ConnectionError, RuntimeError, OSError) as exc:
                self.events.put(("status", f"Could not send CQ frame: {exc}"))
        elif command == "/disconnect":
            try:
                self.data.disconnect_peer()
                self.events.put(("status", "Disconnecting Data2G ARQ session"))
            except (ConnectionError, RuntimeError, OSError) as exc:
                self.events.put(("status", f"Could not disconnect: {exc}"))
        elif command == "/image-get":
            try:
                identity, separator, sequence = argument.rpartition(":")
                attachment = self.store.find_attachment(identity, int(sequence)) if separator else None
            except ValueError:
                attachment = None
            if not attachment:
                self.events.put(("status", "Use /image-get origin-prefix:sequence for a shown attachment"))
            elif self.store.attachment_blob_for(attachment["digest"]):
                self.events.put(("status", "Attachment already downloaded"))
            else:
                self._request_thumbnail(attachment, attempt=0)
        elif command == "/image":
            try:
                image_args = shlex.split(argument)
            except ValueError as exc:
                self.events.put(("status", f"Invalid image path quoting: {exc}"))
                return True
            if not image_args:
                self.events.put(("status", "Usage: /image PATH [caption]"))
            else:
                self.queue_image(image_args[0], " ".join(image_args[1:]))
        elif command == "/help":
            self.events.put(("status", "/join #channel  /nick name  /auth CALLSIGN  /cq  /connect CALLSIGN  /disconnect  "
                             "/search TEXT  /images  "
                             "/image PATH [caption]  "
                             "/image-get origin-prefix:sequence  /history [n]  /channels  /sync  "
                             "/clear-history  /quit"))
        elif command.startswith("/"):
            self.events.put(("status", "Unknown command; try /help"))
        else:
            self.publish(line)
        return True

    def clear_local_history(self) -> int:
        """Purge local records and forget transient sync state for this run."""
        self.last_purge_queued_frames = self.data.discard_queued_transmissions()
        with self.pending_lock:
            for cache in (self.pending, self.pending_sync_requests,
                          self.completed_sync_windows,
                          self.pending_sync_windows, self.served_sync_requests,
                          self.sync_repair_attempts):
                cache.clear()
        with self.auth_pending_lock:
            self.auth_pending.clear()
            self.auth_checked.clear()
            self.signing_jobs.clear()
            self.reference_jobs.clear()
            self.signing_retry_after.clear()
        return self.store.clear_local_history()

    def _mentioned_here(self, body: str) -> list[str]:
        identities = {self.args.callsign.casefold(), self.nick.casefold()}
        mentions = []
        seen = set()
        for match in MENTION.finditer(body):
            name = mention_name(match)
            folded = name.casefold()
            if folded in identities and folded not in seen:
                mentions.append(name)
                seen.add(folded)
        return mentions

    def _send_auth_reference(self, origin: str, seq: int, reference: str) -> None:
        self.store.set_auth_reference(origin, seq, reference)
        auth = self.store.auth_for(origin, seq)
        self.events.put(("auth", {"o": origin, "s": int(seq),
                                   "status": auth["status"],
                                   "signer": auth["signer"],
                                   "detail": auth["detail"],
                                   "reference": reference}))
        if self._route_once(("a", origin, int(seq), reference), 4):
            self.send_record({"t": "A", "o": origin, "s": int(seq),
                              "r": reference, "ttl": 4}, priority=10)

    def _send_auth_challenge(self, kind: str, fields: dict, payload: bytes) -> bool:
        if not payload or len(payload) > AUTH_CHALLENGE_MAX_BYTES:
            raise ValueError("ARAP challenge data exceeds the 4096-byte radio limit")
        chunks = [payload[index:index + AUTH_CHALLENGE_CHUNK_BYTES]
                  for index in range(0, len(payload), AUTH_CHALLENGE_CHUNK_BYTES)]
        if len(chunks) > 40:
            raise ValueError("ARAP challenge requires too many radio fragments")
        for index, chunk in enumerate(chunks):
            record = {**fields, "t": kind, "i": index, "z": len(chunks), "b": chunk}
            if not self.send_record(record, priority=10):
                return False
            self._route_once((kind.lower(), fields["id"], index),
                             int(fields.get("ttl", 4)))
        return True

    def _accept_auth_challenge_fragment(self, kind: str, record: dict) -> bytes | None:
        identifier = record.get("id")
        index, count, chunk = record.get("i"), record.get("z"), record.get("b")
        if (not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{16}", identifier) or
                type(index) is not int or type(count) is not int or
                not 1 <= count <= 40 or not 0 <= index < count or
                not isinstance(chunk, bytes) or len(chunk) > AUTH_CHALLENGE_CHUNK_BYTES):
            return None
        metadata = {key: record.get(key) for key in ("from", "to", "c")}
        now = time.monotonic()
        key = (kind, identifier)
        with self.auth_fragment_lock:
            for old_key, transfer in list(self.auth_fragments.items()):
                if transfer["expires"] <= now:
                    self.auth_fragments.pop(old_key, None)
            if key not in self.auth_fragments and len(self.auth_fragments) >= 64:
                self.auth_fragments.pop(next(iter(self.auth_fragments)))
            transfer = self.auth_fragments.setdefault(
                key, {"count": count, "metadata": metadata, "chunks": {},
                      "expires": now + 600})
            if transfer["count"] != count or transfer["metadata"] != metadata:
                self.auth_fragments.pop(key, None)
                return None
            existing = transfer["chunks"].get(index)
            if existing is not None and existing != chunk:
                self.auth_fragments.pop(key, None)
                return None
            transfer["chunks"][index] = chunk
            if len(transfer["chunks"]) != count:
                return None
            payload = b"".join(transfer["chunks"][part] for part in range(count))
            self.auth_fragments.pop(key, None)
            return payload if len(payload) <= AUTH_CHALLENGE_MAX_BYTES else None

    def _issue_auth_challenge(self, responder: str) -> None:
        responder = responder.upper()
        if not re.fullmatch(r"(?=.{3,20}$)(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]+(?:/[A-Z0-9]+)*",
                            responder):
            self.events.put(("status", "Usage: /auth CALLSIGN (ARAP callsign format)"))
            return
        if not self.auth.enabled:
            self.events.put(("status", "Configure an ARAP key before requesting authentication"))
            return
        temporary = os.path.join(self.auth_request_dir, f"request-{secrets.token_hex(8)}.cbor")
        try:
            os.makedirs(self.auth_request_dir, mode=0o700, exist_ok=True)
            issued = self.auth.issue_challenge(responder, self.auth_challenge_store, temporary)
            request = open(temporary, "rb").read()
            full_hash = issued["request_sha256"]
            if hashlib.sha256(request).hexdigest() != full_hash:
                raise ValueError("ARAP challenge request hash did not match its bytes")
            safe_call = responder.replace("/", "-")
            destination = os.path.join(self.auth_request_dir,
                                       f"{full_hash}-{safe_call}.cbor")
            os.replace(temporary, destination)
            identifier = full_hash[:16]
            queued = self._send_auth_challenge("B", {"from": self.store.node,
                                                       "c": self.auth.callsign, "to": responder,
                                                       "id": identifier, "ttl": 4}, request)
            if not queued:
                self.events.put(("status", "ARAP authentication request was only partly queued"))
                return
            self.events.put(("status", f"One-time ARAP authentication requested from {responder}"))
        except Exception as exc:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            self.events.put(("status", f"Could not request ARAP authentication: {exc}"))

    def _answer_auth_challenge(self, identifier: str, requester_node: str,
                               request: bytes) -> None:
        try:
            response = self.auth.respond_challenge(request)
            if not self._send_auth_challenge("C", {"from": self.store.node,
                                                     "c": self.auth.callsign,
                                                     "to": requester_node,
                                                     "id": identifier, "ttl": 4}, response):
                self.events.put(("status", "ARAP authentication response was only partly queued"))
                return
            self.events.put(("status", f"Sent one-time ARAP authentication response to "
                             f"{requester_node[:8]}"))
        except Exception as exc:
            self.events.put(("status", f"Could not answer ARAP authentication request: {exc}"))

    def _verify_auth_challenge(self, identifier: str, responder: str,
                               response: bytes) -> None:
        try:
            candidates = [name for name in os.listdir(self.auth_request_dir)
                          if name.startswith(identifier) and name.endswith(".cbor")]
        except OSError:
            candidates = []
        if not candidates:
            self.events.put(("status", "ARAP authentication response has no pending request"))
            return
        last_error = "no matching pending challenge"
        policy_revision = self.auth_policy_revision
        for name in candidates[:8]:
            stem = name[:-5]
            request_path = os.path.join(self.auth_request_dir, name)
            requested = stem.split("-", 1)[1].replace("-", "/") if "-" in stem else ""
            if requested.upper() != responder.upper():
                continue
            try:
                self.auth.verify_challenge(request_path, response, responder,
                                           self.auth_challenge_store)
                if self.auth.verification_revision() != policy_revision:
                    raise ValueError("ARAP trust policy changed during challenge verification")
                now = int(time.time())
                self.store.record_station_auth(responder, now)
                self.events.put(("station-auth", {"callsign": responder,
                                                   "verified_at": now}))
                try:
                    os.unlink(request_path)
                except OSError:
                    pass
                return
            except Exception as exc:
                last_error = str(exc)
        self.events.put(("status", f"ARAP authentication from {responder} failed: {last_error}"))

    def _handle_auth_challenge_record(self, kind: str, record: dict) -> None:
        sender, recipient, callsign = record.get("from"), record.get("to"), record.get("c")
        identifier = record.get("id")
        if (not isinstance(sender, str) or not NODE_ID.fullmatch(sender) or
                not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{16}", identifier) or
                not isinstance(callsign, str)):
            return
        ttl = int(record.get("ttl", 4))
        index = record.get("i")
        if type(index) is not int:
            return
        route_key = (kind.lower(), identifier, index)
        if ttl > 0 and (kind == "B" or recipient != self.store.node) and \
                self._route_once(route_key, ttl):
            forwarded = dict(record)
            forwarded["ttl"] = ttl - 1
            self.send_record(forwarded, priority=10)
        if kind == "B":
            if str(recipient).upper() != self.auth.callsign or not self.auth.enabled:
                return
            payload = self._accept_auth_challenge_fragment(kind, record)
            if payload is None:
                return
            if hashlib.sha256(payload).hexdigest()[:16] != identifier:
                return
            now = time.monotonic()
            with self.auth_fragment_lock:
                if self.auth_response_cooldown.get(sender, 0) > now:
                    return
                self.auth_response_cooldown[sender] = now + 60
                if len(self.auth_response_cooldown) > 512:
                    self.auth_response_cooldown.pop(next(iter(self.auth_response_cooldown)))
            self.auth_executor.submit(self._answer_auth_challenge,
                                      identifier, sender, payload)
        elif kind == "C" and recipient == self.store.node:
            payload = self._accept_auth_challenge_fragment(kind, record)
            if payload is None:
                return
            self.auth_executor.submit(self._verify_auth_challenge,
                                      identifier, callsign.upper(), payload)

    def _queue_reference_resolution(self, origin: str, seq: int, reference: str) -> None:
        key = (origin, int(seq))
        if self.store.proof_for(*key) is not None:
            proof = self.store.proof_for(*key)
            if proof:
                self._verify_stored_proof(origin, seq, proof)
            return
        if not self.auth.servers:
            self.store.set_auth(origin, seq, None, "unverified", "",
                                "AR1 reference received; no ARAP resolver configured")
            return
        with self.auth_pending_lock:
            if key in self.reference_jobs or len(self.reference_jobs) >= MAX_PENDING_AUTH_JOBS:
                return
            self.reference_jobs.add(key)
        try:
            self.auth_executor.submit(self._resolve_auth_reference, origin, seq, reference)
        except RuntimeError:
            with self.auth_pending_lock:
                self.reference_jobs.discard(key)

    def _resolve_auth_reference(self, origin: str, seq: int, reference: str) -> None:
        key = (origin, int(seq))
        try:
            proof = self.auth.resolve(reference)
            if not self.store.has_message(origin, seq):
                return
            self.store.set_auth(origin, seq, proof, "pending")
            self._verify_stored_proof(origin, seq, proof)
        except Exception as exc:
            if self.store.has_message(origin, seq):
                self.store.set_auth(origin, seq, None, "unverified", "",
                                    f"AR1 proof lookup failed: {exc}")
                self.events.put(("auth", {"o": origin, "s": seq,
                                           "status": "unverified", "signer": "",
                                           "detail": f"AR1 proof lookup failed: {exc}",
                                           "reference": reference}))
        finally:
            with self.auth_pending_lock:
                self.reference_jobs.discard(key)

    def _create_receipt(self, message: dict, mention: str) -> None:
        received_at = int(time.time())
        self.store.add_relay_receipt(message["o"], int(message["s"]), self.store.node,
                                     self.args.callsign, mention, received_at)
        self._forward_pending_receipts()

    def _forward_pending_receipts(self) -> None:
        for receipt in self.store.claim_pending_receipts(int(time.time())):
            self.send_record({"t": "r", "to": receipt["origin"], "o": receipt["origin"],
                              "s": receipt["seq"], "p": receipt["recipient_id"],
                              "from": self.store.node, "n": receipt["nick"],
                              "m": receipt["mention"], "at": receipt["received_at"]})

    def _send_attachment(self, message: dict) -> None:
        image = message["image"]
        record = {"t": "a", "o": message["o"], "s": message["s"],
                  "h": image["digest"], "b": image["size"],
                  "w": image["width"], "hgt": image["height"], "ttl": 4}
        self._send_routed_record(record, ("a", "", message["o"], message["s"],
                                          image["digest"]), operator_initiated=True)

    def _send_image(self, recipient: str, origin: str, seq: int, digest: str,
                    body: bytes, request_id: str, indexes: list[int] | None = None) -> None:
        try:
            for frame in image_parts(recipient=recipient, origin=origin, seq=seq,
                                     digest=digest, data=body,
                                     request_id=request_id, indexes=indexes):
                record = unpack(frame)
                key = (request_id, recipient, origin, seq, digest, int(record["i"]))
                with self.image_lock:
                    # Hearing a downstream relay must not make the sender
                    # transmit its own chunk back into the same transfer.
                    self.seen_image_chunks[key] = (int(record["ttl"]), time.monotonic() + 600)
                    self._bound_timed_map(self.seen_image_chunks, time.monotonic(),
                                          MAX_IMAGE_CHUNKS_SEEN)
                self.data.send(frame, priority=10)
        except (OSError, ValueError, queue.Full, ConnectionError) as exc:
            self.events.put(("status", f"Could not queue image: {exc}"))

    def _receive_image_part(self, record: dict) -> None:
        origin, seq, digest = record["o"], int(record["s"]), record["h"]
        key = (origin, seq, digest)
        attachment = self.store.attachment_for(origin, seq)
        if not attachment or attachment["digest"] != digest:
            return
        cached = self.store.attachment_blob_for(digest)
        if cached is not None:
            if attachment.get("kind") == "card":
                try:
                    reference = decode_pixel_card_reference(attachment.get("metadata", ""))
                    if reference is not None:
                        self._save_received_card_bytes(cached, reference, origin, seq)
                except ValueError:
                    pass
            return
        kind = attachment.get("kind", "image")
        validator = None
        reference = None
        if kind == "card":
            try:
                reference = decode_pixel_card_reference(attachment.get("metadata", ""))
            except ValueError:
                return
            if (reference is None or reference.digest != digest or
                    reference.size != int(attachment["size"])):
                return

            def validator(payload: bytes) -> bool:
                verify_pixel_card_payload(payload, reference)
                return True

        body = self.store.add_attachment_fragment(record, kind=kind, validator=validator)
        count = (int(attachment["size"]) + 138) // 139
        missing = self.store.missing_image_indices(origin, seq, count) if body is None else []
        if body is None and int(record["i"]) in missing:
            return
        identity = {"o": origin, "s": seq}
        # All listeners cache and display overheard images. Only an existing
        # explicit download may schedule the next window or a repair.
        self.events.put(("image-progress", identity))
        advance = False
        with self.image_lock:
            if body is not None:
                self.pending_images.pop(key, None)
                self.image_windows.pop(key, None)
            elif key in self.pending_images:
                window = self.image_windows.get(key, set())
                if int(record["i"]) in window:
                    window.discard(int(record["i"]))
                    attempt, _, expires = self.pending_images[key]
                    self.pending_images[key] = (attempt, time.monotonic() + self._image_retry_interval(),
                                                expires)
                    advance = not window
        if body is not None:
            if kind == "card" and reference is not None:
                self._save_received_card_bytes(body, reference, origin, seq)
                self.events.put(("attachment", identity))
            else:
                self.events.put(("image", identity))
        elif advance:
            self._request_thumbnail(attachment, attempt=0, continuing=True)

    def _receiving_image_window(self) -> bool:
        with self.image_lock:
            return any(self.image_windows.values())

    def _request_thumbnail(self, attachment: dict, *, attempt: int,
                           continuing: bool = False) -> int:
        """Request one small attachment window; chunks acknowledge progress."""
        origin, seq, digest = (attachment["origin"], int(attachment["seq"]),
                               attachment["digest"])
        count = (int(attachment["size"]) + 138) // 139
        missing = self.store.missing_image_indices(origin, seq, count)
        cached = self.store.attachment_blob_for(digest)
        if not missing or cached is not None:
            if cached is not None and attachment.get("kind") == "card":
                try:
                    reference = decode_pixel_card_reference(attachment.get("metadata", ""))
                    if reference is not None:
                        self._save_received_card_bytes(cached, reference, origin, seq)
                except ValueError:
                    pass
            return 0
        # Give new chat inventories a turn before asking the sender for the
        # next burst. Polling while that burst arrives causes hidden-terminal
        # collisions between the image source and its own requester.
        self._flush_relay_announcements(image_boundary=True)
        now = time.monotonic()
        key = (origin, seq, digest)
        with self.image_lock:
            self._bound_timed_map(self.seen_image_requests, now, MAX_IMAGE_REQUESTS)
            if (attempt == 0 and key not in self.pending_images and
                    len(self.pending_images) >= MAX_PENDING_IMAGES):
                self.events.put(("status", "Too many attachment requests are already pending"))
                return 0
            previous = self.pending_images.get(key)
            if continuing and previous is None:
                return 0
            window = sorted(self.image_windows.get(key, set()).intersection(missing))
            batch = window if attempt > 0 and window else missing[:IMAGE_REQUEST_WINDOW]
            expires = previous[2] if previous and (continuing or attempt > 0) else now + 600
            if expires <= now:
                self.pending_images.pop(key, None)
                self.image_windows.pop(key, None)
                return 0
            self.pending_images[key] = (attempt, now + IMAGE_RESPONSE_SECONDS, expires)
            self.image_windows[key] = set(batch)
        request_id = secrets.token_hex(4)
        request_key = (self.store.node, origin, seq, digest, request_id)
        with self.image_lock:
            self.seen_image_requests[request_key] = (4, now + 120)
            self._bound_timed_map(self.seen_image_requests, now, MAX_IMAGE_REQUESTS)
        record = {"t": "g", "p": self.store.node, "o": origin, "s": seq,
                  "h": digest, "id": request_id, "ttl": 4, "i": batch}
        # JSON requests with larger sequence/index numbers fit fewer entries.
        # Keep each request inside the existing wire limit without flooding
        # several repair batches into the channel at once.
        while len(batch) > 1:
            try:
                pack(record)
                break
            except ValueError:
                batch = batch[:-1]
                record["i"] = batch
        with self.image_lock:
            self.image_windows[key] = set(batch)
        queued = int(self.send_record(record))
        if not queued:
            with self.image_lock:
                self.pending_images[key] = (attempt, now + 30, expires)
        if attempt == 0 and not continuing:
            attachment_label = "PixelQSO card" if attachment.get("kind") == "card" else "Image"
            if queued:
                self.events.put(("status", f"{attachment_label} requested from nearby stations"))
            else:
                self.events.put(("status", f"{attachment_label} request could not be queued; try again"))
        return queued

    def _save_received_card_bytes(self, payload: bytes, reference, origin: str,
                                  seq: int) -> bool:
        try:
            received_card = verify_pixel_card_payload(payload, reference)
            save_received_pixel_card_message(received_card, origin=origin, seq=seq)
        except (OSError, ValueError) as exc:
            self.events.put(("status", f"Could not save PixelQSO received card: {exc}"))
            return False
        return True

    def _retry_pending_images(self) -> None:
        now = time.monotonic()
        with self.image_lock:
            due = [(key, state) for key, state in self.pending_images.items()
                   if state[1] <= now]
            for key, (_, _, expires) in list(self.pending_images.items()):
                if expires <= now:
                    self.pending_images.pop(key, None)
                    self.image_windows.pop(key, None)
        for (origin, seq, digest), (attempt, _, expires) in due:
            with self.image_lock:
                current = self.pending_images.get((origin, seq, digest))
                if current is None or current[1] > now:
                    continue
                if attempt >= 3 or expires <= now:
                    self.pending_images.pop((origin, seq, digest), None)
                    self.image_windows.pop((origin, seq, digest), None)
                    self.events.put(("status", f"Attachment {origin[:8]}:{seq} still incomplete; "
                                    "select it to request again"))
                    continue
            attachment = self.store.attachment_for(origin, seq)
            if (not attachment or attachment["digest"] != digest or
                    self.store.attachment_blob_for(digest)):
                with self.image_lock:
                    self.pending_images.pop((origin, seq, digest), None)
                    self.image_windows.pop((origin, seq, digest), None)
                continue
            queued = self._request_thumbnail(attachment, attempt=attempt + 1)
            if queued:
                self.events.put(("status", f"Retrying attachment {origin[:8]}:{seq}; "
                                "requesting missing fragments"))
            else:
                with self.image_lock:
                    self.pending_images[(origin, seq, digest)] = (attempt, now + 30, expires)

    def _forward_image_part(self, record: dict) -> None:
        try:
            self.data.send(image_part(recipient=record["to"], origin=record["o"],
                                      seq=int(record["s"]), digest=record["h"],
                                      index=int(record["i"]), count=int(record["z"]),
                                      chunk=record["b"], request_id=record["id"],
                                      ttl=int(record["ttl"]) - 1), priority=10)
        except (OSError, ValueError, queue.Full, ConnectionError) as exc:
            self.events.put(("status", f"Could not relay image fragment: {exc}"))

    def _schedule_image_response(self, request_key: tuple[str, str, int, str, str],
                                 recipient: str, attachment: dict, body: bytes,
                                 indexes: list[int] | None = None) -> None:
        with self.image_lock:
            claim = self.image_claims.get(request_key)
            if claim and claim[1] > time.monotonic():
                return
            pending = self.image_response_timers.get(request_key)
            if pending and pending.is_alive():
                return
            for key, timer in list(self.image_response_timers.items()):
                if not timer.is_alive():
                    self.image_response_timers.pop(key, None)
            if (request_key not in self.image_response_timers and
                    len(self.image_response_timers) >= MAX_IMAGE_RESPONSE_TIMERS):
                return
            # A stable per-station delay lets a nearby cache claim the
            # response while giving that claim time to cross the mesh.
            delay = 1.0 + (int(self.store.node[-4:], 16) % 5000) / 1000
            timer = threading.Timer(delay, self._claim_and_send_image,
                                    args=(request_key, recipient, attachment, body, indexes))
            timer.daemon = True
            self.image_response_timers[request_key] = timer
            timer.start()

    def _claim_and_send_image(self, request_key: tuple[str, str, int, str, str],
                              recipient: str, attachment: dict, body: bytes,
                              indexes: list[int] | None = None) -> None:
        with self.image_lock:
            claim = self.image_claims.get(request_key)
            now = time.monotonic()
            if (claim and claim[1] > now and
                    claim[0] <= self.store.node):
                self.image_response_timers.pop(request_key, None)
                return
            claimant = self.store.node
            self.image_claims[request_key] = (claimant, now + 120)
            self._bound_timed_map(self.image_claims, now, MAX_IMAGE_REQUESTS)
            # Hold the chunks briefly after claiming. Other relays may have
            # heard a competing claim through a longer path; the settle timer
            # lets the winner update propagate before any cache starts TX.
            timer = threading.Timer(2.0, self._send_claimed_image,
                                   args=(request_key, recipient, attachment, body, indexes))
            timer.daemon = True
            self.image_response_timers[request_key] = timer
            timer.start()
        self.send_record({"t": "y", "p": recipient, "o": attachment["origin"],
                          "s": attachment["seq"], "h": attachment["digest"],
                          "id": request_key[4], "by": claimant, "ttl": 4})

    def _send_claimed_image(self, request_key: tuple[str, str, int, str, str],
                            recipient: str, attachment: dict, body: bytes,
                            indexes: list[int] | None = None) -> None:
        with self.image_lock:
            claim = self.image_claims.get(request_key)
            self.image_response_timers.pop(request_key, None)
            if not claim or claim[1] <= time.monotonic() or claim[0] != self.store.node:
                return
        self._send_image(recipient, attachment["origin"], attachment["seq"],
                         attachment["digest"], body, request_key[4], indexes)

    def _verify_stored_proof(self, origin: str, seq: int, proof: bytes) -> None:
        # Look up the exact message through the common history query so relay
        # and live-receive paths apply the same signed-content canonicalization.
        rows = self.store.messages_between(origin, seq - 1, seq)
        if not rows:
            return
        message = rows[0]
        attachment = self.store.attachment_for(origin, seq)
        if attachment:
            message["image"] = {"digest": attachment["digest"], "size": attachment["size"],
                                "width": attachment["width"], "height": attachment["height"]}
        key = (origin, seq)
        policy_revision = self.auth_policy_revision
        fingerprint = self._auth_fingerprint(message, policy_revision)
        with self.auth_pending_lock:
            pending = (proof, fingerprint)
            if self.auth_checked.get(key) == pending:
                return
            if self.auth_pending.get(key) == pending:
                return
            if len(self.auth_pending) >= MAX_PENDING_AUTH_JOBS:
                return
            self.auth_pending[key] = pending
        try:
            self.auth_executor.submit(self._verify_proof, message, proof, fingerprint,
                                      policy_revision)
        except RuntimeError:
            with self.auth_pending_lock:
                if self.auth_pending.get(key) == pending:
                    self.auth_pending.pop(key, None)

    def _retry_pending_verifications(self) -> None:
        proofs = self.store.pending_auth_proofs(MAX_PENDING_AUTH_JOBS,
                                                self.auth_reverify_cursor)
        if not proofs and self.auth_reverify_cursor is not None:
            self.auth_reverify_cursor = None
            proofs = self.store.pending_auth_proofs(MAX_PENDING_AUTH_JOBS)
        for origin, seq, proof in proofs:
            self.auth_reverify_cursor = (origin, seq)
            self._verify_stored_proof(origin, seq, proof)

    def _refresh_auth_policy(self) -> None:
        revision = self.auth.verification_revision()
        with self.auth_pending_lock:
            if revision == self.auth_policy_revision:
                return
            changed = self.store.refresh_auth_policy(revision)
            self.auth_policy_revision = revision
            # Results already running under the previous trust policy must not
            # overwrite the newly invalidated state when they finish.
            self.auth_checked.clear()
            self.auth_pending.clear()
        self.auth_reverify_cursor = None
        if changed:
            self.events.put(("auth-policy", {"count": changed}))

    def _queue_signature(self, message: dict) -> bool:
        key = (message["o"], int(message["s"]))
        with self.auth_pending_lock:
            if key in self.signing_jobs:
                return False
            if len(self.signing_jobs) >= MAX_PENDING_AUTH_JOBS:
                self.events.put(("status", "ARAP signing queue is busy; saved messages will retry"))
                return False
            self.signing_jobs.add(key)
        try:
            self.auth_executor.submit(self._sign_message, message)
            return True
        except RuntimeError as exc:
            with self.auth_pending_lock:
                self.signing_jobs.discard(key)
            self.events.put(("status", f"Could not queue ARAP signing: {exc}"))
            return False

    def _retry_pending_signatures(self) -> None:
        if not self.auth.enabled:
            return
        for message in self.store.pending_local_signatures(
                self.store.node, MAX_PENDING_SYNC_REQUESTS):
            key = (message["o"], int(message["s"]))
            now = time.monotonic()
            with self.auth_pending_lock:
                for old_key, expiry in list(self.signing_retry_after.items()):
                    if expiry <= now:
                        self.signing_retry_after.pop(old_key, None)
                if self.signing_retry_after.get(key, 0) > now:
                    continue
                if len(self.signing_jobs) >= MAX_PENDING_AUTH_JOBS:
                    return
            self._queue_signature(message)

    def _sign_message(self, message: dict) -> None:
        key = (message["o"], int(message["s"]))
        try:
            proof = self.auth.sign(message)
            self.store.set_auth(message["o"], message["s"], proof,
                                "pending", self.args.callsign)
            if self.auth.servers:
                try:
                    reference = self.auth.publish(proof)
                    self._send_auth_reference(message["o"], message["s"], reference)
                except Exception as publish_error:
                    self.events.put(("status", "ARAP signature created, but out-of-band "
                                    f"publication failed: {publish_error}"))
            else:
                self.events.put(("status", "ARAP signing is local only; configure an ARAP "
                                "keyserver to publish proofs out of band"))
            policy_revision = self.auth_policy_revision
            fingerprint = self._auth_fingerprint(message, policy_revision)
            with self.auth_pending_lock:
                self.signing_retry_after.pop(key, None)
                self.auth_pending[key] = (proof, fingerprint)
            self._verify_proof(message, proof, fingerprint, policy_revision)
        except Exception as exc:
            try:
                self.store.set_auth(message["o"], message["s"], None,
                                    "signing_failed", self.args.callsign, str(exc))
            except Exception as store_exc:
                self.events.put(("status", f"Could not save ARAP signing failure: {store_exc}"))
            with self.auth_pending_lock:
                self.signing_retry_after[key] = time.monotonic() + 300
                while len(self.signing_retry_after) > MAX_PENDING_SYNC_REQUESTS:
                    oldest = min(self.signing_retry_after,
                                 key=self.signing_retry_after.get)
                    self.signing_retry_after.pop(oldest, None)
            self.events.put(("auth", {"o": key[0], "s": key[1],
                                       "status": "signing_failed",
                                       "signer": self.args.callsign,
                                       "detail": str(exc)}))
        finally:
            with self.auth_pending_lock:
                self.signing_jobs.discard(key)

    @staticmethod
    def _auth_fingerprint(message: dict, policy_revision: str = "") -> str:
        canonical = json.dumps(message, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256((canonical + "\0" + policy_revision).encode("utf-8")).hexdigest()

    def _verify_proof(self, message: dict, proof: bytes, fingerprint: str,
                      policy_revision: str) -> None:
        key = (message["o"], int(message["s"]))
        try:
            status, signer, detail = self.auth.verify(message, proof)
        except ARAPUnavailable as exc:
            status, signer, detail = "unverified", "", str(exc)
        except Exception as exc:
            status, signer, detail = "unverified", "", str(exc)
        try:
            with self.auth_pending_lock:
                current = self.auth_pending.get(key)
                if (current == (proof, fingerprint) and
                        self.auth_policy_revision == policy_revision and
                        self.store.proof_for(*key) == proof):
                    self.store.set_auth(key[0], key[1], proof, status, signer, detail)
                    self.auth_checked[key] = (proof, fingerprint)
                    if len(self.auth_checked) > 8192:
                        self.auth_checked.pop(next(iter(self.auth_checked)))
                    self.events.put(("auth", {"o": key[0], "s": key[1], "status": status,
                                              "signer": signer, "detail": detail,
                                              "reference": self.store.auth_for(*key).get(
                                                  "reference", "")}))
        finally:
            with self.auth_pending_lock:
                if self.auth_pending.get(key) == (proof, fingerprint):
                    self.auth_pending.pop(key, None)

    def _message_line(self, message):
        stamp = time.strftime("%H:%M:%S", time.localtime(int(message["ts"])))
        line = f"[{stamp}] <{message['n']}> {message['b']}"
        if message["o"] == self.store.node:
            mentions = [mention_name(match) for match in MENTION.finditer(message["b"])]
            if mentions:
                received = self.store.receipts_for(message["o"], message["s"])
                states = []
                for name in mentions:
                    stations = [station for station, mention in received
                                if mention.casefold() == name.casefold()]
                    label = format_mention(name)
                    states.append(f"✓ {label} ({','.join(stations)})" if stations
                                  else f"… {label}")
                line += "  [" + ", ".join(states) + "]"
        auth = self.store.auth_for(message["o"], message["s"])
        if auth["status"] == "pending" and auth["proof"] is None:
            line += ("  ◌ Resolving ARAP proof" if auth.get("reference")
                     else "  ◌ Signing with ARAP")
        elif auth["status"] == "pending" and auth["proof"] is not None:
            signer_label = f" · {auth['signer']}" if auth["signer"] else ""
            line += f"  ◌ Checking signature{signer_label}"
        elif auth["status"] == "verified":
            line += f"  ✓ Verified · {auth['signer']}"
        elif auth["status"] == "signing_failed":
            line += "  ! Sent unsigned · ARAP signing failed (will retry)"
        elif auth["status"] == "untrusted":
            line += f"  ◌ Signed · {auth['signer']} · untrusted"
        elif auth["status"] == "unverified":
            line += f"  ◌ Signature unchecked · claimed signer {auth['signer']}"
        elif auth["status"] == "invalid":
            line += "  ! Invalid signature"
        return line

    def close(self):
        self.sync_stop.set()
        if self.sync_thread and self.sync_thread is not threading.current_thread():
            self.sync_thread.join(timeout=2)
        with self.image_lock:
            for timer in self.image_response_timers.values():
                timer.cancel()
            self.image_response_timers.clear()
        self.image_executor.shutdown(wait=True, cancel_futures=False)
        self.data.close()
        self.auth_executor.shutdown(wait=True, cancel_futures=False)
        self.store.close()


def _handle_sigterm(_signum, _frame):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description="IRC-like channel chat over KISS radio links")
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    parser.add_argument("--host", default=os.getenv(
        "KISSCHAT_HOST", os.getenv("DATA2G_HOST", "127.0.0.1")),
        help="Data2G or raw KISS TNC host/IP")
    parser.add_argument("--callsign", required=True,
                        help="on-air station callsign (AX.25 allows 1–6 characters plus optional -SSID)")
    parser.add_argument("--nick", help="display nickname; defaults to callsign")
    parser.add_argument("--channel", help="initial channel (defaults to last used, then #general)")
    parser.add_argument("--mode", default="",
                        help="Data2G broadcast mode; in unattended mode this is used for explicit operator sends")
    parser.add_argument("--unattended", action="store_true",
                        help="use a host-advertised Data2G broadcast mode at or below 500 Hz for automatic sync/relay; explicit sends may use --mode or the widest host mode")
    parser.add_argument("--transport", choices=("broadcast", "connected", "ax25"), default="broadcast",
                        help="Data2G broadcast, VARA-compatible connected ARQ, or AX.25 UI over raw KISS")
    parser.add_argument("--bandwidth", choices=("500", "1200", "2300", "2750"),
                        default="2300", help="Data2G connected ARQ bandwidth cap in Hz")
    parser.add_argument("--max-bandwidth", type=int,
                        help="maximum Data2G broadcast mode bandwidth in Hz; selects the widest advertised mode within the limit")
    parser.add_argument("--command-port", type=int, default=8300)
    parser.add_argument("--kiss-port", type=int, default=(
        int(os.environ["KISSCHAT_KISS_PORT"])
        if os.environ.get("KISSCHAT_KISS_PORT") else None),
                        help="raw KISS TCP port (default 8001 for ax25; Data2G uses 8100)")
    parser.add_argument("--kiss-tnc-port", type=int,
                        default=int(os.getenv("KISSCHAT_KISS_TNC_PORT") or "0"),
                        help="KISS TNC channel number for ax25 transport (default 0)")
    parser.add_argument("--ax25-destination",
                        default=os.getenv("KISSCHAT_AX25_DESTINATION", "CQ"),
                        help="AX.25 UI destination callsign/SSID for ax25 transport (default CQ)")
    parser.add_argument("--digipeaters", default=os.getenv("KISSCHAT_DIGIPEATERS", ""),
                        help="optional comma-separated AX.25 digipeater path, e.g. WIDE1-1,WIDE2-1")
    parser.add_argument("--ax25-baud", type=int,
                        default=int(os.getenv("KISSCHAT_AX25_BAUD") or "1200"),
                        help="estimated AX.25 radio bit rate for KISS queue pacing (default 1200)")
    parser.add_argument("--ax25-interframe-delay", type=float,
                        default=float(os.getenv("KISSCHAT_AX25_INTERFRAME_DELAY") or "0.25"),
                        help="extra seconds between estimated AX.25 frames (default 0.25)")
    parser.add_argument("--db", default=os.path.expanduser("~/.local/share/kisschat/chat.sqlite3"))
    parser.add_argument("--sync-history-hours", type=int,
                        help=f"maximum age of history to advertise/request/serve (1-{MAX_SYNC_HISTORY_HOURS} hours; default 48; saved in the database)")
    parser.add_argument("--sync-history-messages", type=int,
                        help=f"maximum messages requested across all origins per sync exchange (default 36; hard cap {SYNC_SPARSE_MESSAGES}; 0 uses cap; saved in the database)")
    parser.add_argument("--debug-log", default="",
                        help="append timestamped transport and protocol events to this file")
    parser.add_argument("--arap-cli", default=os.getenv("ARAP_CLI", "arap"),
                        help="ARAP executable used for message signatures")
    parser.add_argument("--arap-key", default=os.getenv("KISSCHAT_ARAP_KEY", ""),
                        help="ARAP Ed25519 private key; signing stays disabled if omitted; "
                             "proofs publish out of band when --arap-server is configured")
    parser.add_argument("--arap-server", action="append",
                        default=[value.strip() for value in re.split(
                            r"[;,]", os.getenv("KISSCHAT_ARAP_SERVERS", "")) if value.strip()],
                        help="ARAP keyserver URL (repeat for resolvers); signing uploads proofs "
                             "and verification fetches proofs out of band, while only AR1 goes over radio")
    parser.add_argument("--arap-key-dir", default=os.getenv(
        "KISSCHAT_ARAP_KEY_DIR", os.path.expanduser("~/.config/kisschat/keys")),
        help="directory containing CALLSIGN.pub.pem trusted-key candidates; missing proofs are "
             "fetched from configured ARAP servers over the network")
    parser.add_argument("--arap-trust-store", default=os.getenv(
        "KISSCHAT_ARAP_TRUST_STORE", os.path.expanduser("~/.config/arap/trust.json")),
        help="local ARAP trust store used for verified status")
    parser.add_argument("--arap-passphrase-env", default="KISSCHAT_ARAP_PASSPHRASE",
                        help="environment variable read by ARAP for encrypted-key signing")
    parser.add_argument("--headless", action="store_true",
                        help="stay connected and relay/sync history without a terminal UI")
    parser.add_argument("--auto-sync", dest="auto_sync", action="store_true",
                        help="periodically exchange history (the default; transmits on air)")
    parser.add_argument("--no-auto-sync", dest="auto_sync", action="store_false",
                        help="disable periodic history exchange")
    parser.set_defaults(auto_sync=True)
    args = parser.parse_args()
    if args.sync_history_hours is not None and not 1 <= args.sync_history_hours <= MAX_SYNC_HISTORY_HOURS:
        parser.error(f"--sync-history-hours must be between 1 and {MAX_SYNC_HISTORY_HOURS}")
    if args.sync_history_messages is not None and not 0 <= args.sync_history_messages <= 1_000_000:
        parser.error("--sync-history-messages must be between 0 and 1000000")
    if args.debug_log:
        args.debug_log = os.path.abspath(os.path.expanduser(args.debug_log))
        try:
            os.makedirs(os.path.dirname(args.debug_log), exist_ok=True)
            with open(args.debug_log, "a", encoding="utf-8"):
                pass
        except OSError as exc:
            parser.error(f"cannot open debug log: {exc}")
    chat = Chat(args)
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
        chat.start()
        if args.headless:
            while True:
                time.sleep(0.5)
                while True:
                    try:
                        event, value = chat.events.get_nowait()
                    except queue.Empty:
                        break
                    if event == "status":
                        print(value, flush=True)
                    elif event == "message":
                        print(chat._message_line(value), flush=True)
                    elif event == "receipt":
                        print(f"@{value['m']} delivered to {value['n']} "
                              f"(message #{value['s']})", flush=True)
                    elif event == "refresh":
                        print("Event backlog coalesced; stored history remains available",
                              flush=True)
        else:
            from .tui import ChatApp
            ChatApp(chat).run()
    except KeyboardInterrupt:
        pass
    finally:
        chat.close()
