#!/usr/bin/env python3
"""Run three real KISSChat cores over a lossy, timed in-process radio mesh.

This never opens a modem socket. Use commands such as ``send A hello`` and
``sync B`` at the prompt. A command file can be supplied with --script.
"""
from __future__ import annotations

import argparse
from io import BytesIO
from collections import deque
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import queue
import random
import tempfile
import threading
import time
from types import SimpleNamespace

from PIL import Image

from kisschat.app import Chat
from kisschat.ax25 import (decode_ui_frame, encode_ui_frame, pack_batch,
                           unpack_batch)
from kisschat.data2g import KISS_DATA, KissDecoder, encode_kiss
from kisschat.protocol import message_parts, unpack
from kisschat.store import Store


STATIONS = ("A", "B", "C")
CALLSIGNS = {"A": "K0AAA", "B": "K0BBB", "C": "K0CCC"}


@dataclass
class Transmission:
    station: str
    start: float
    duration: float
    wire: bytes
    records: tuple[bytes, ...]
    blocked: set[str] = field(default_factory=set)


class Mesh:
    def __init__(self, *, loss: float, bitrate: int, propagation: float,
                 interframe: float, framing_bytes: int, contention_window: float,
                 time_scale: float, seed: int, topology: str,
                 loss_model: str, burst_recovery: float,
                 transport: str,
                 trace_sync: bool, drop_first_history_fragment: bool,
                 drop_first_history_page_end: bool,
                 drop_first_image_fragment: bool,
                 drop_first_proof_fragment: bool,
                 delay_first_history_page_end: float = 0.0):
        self.loss = loss
        self.loss_model = loss_model
        self.burst_recovery = burst_recovery
        self.bad_links: set[tuple[str, str]] = set()
        self.burst_enter_probability = (
            1.0 if loss >= 1 else loss * burst_recovery / (1 - loss))
        self.bitrate = bitrate
        self.propagation = propagation
        self.interframe = interframe
        self.framing_bytes = framing_bytes
        self.contention_window = contention_window
        self.time_scale = time_scale
        self.transport = transport
        self.trace_sync = trace_sync
        self.drop_first_history_fragment = drop_first_history_fragment
        self.drop_first_history_page_end = drop_first_history_page_end
        self.delay_first_history_page_end = delay_first_history_page_end
        self.delay_history_page_end_armed = False
        self.delayed_history_page_end = False
        self.delayed_history_page_end_count = 0
        self.delayed_history_retry_seen = False
        self.history_data_after_delayed_retry = 0
        self.drop_first_image_fragment = drop_first_image_fragment
        self.drop_first_proof_fragment = drop_first_proof_fragment
        self.dropped_history_fragment = False
        self.dropped_history_page_end = False
        self.dropped_image_fragment = False
        self.image_drop_armed = False
        self.image_drop_match: tuple[str, str, int] | None = None
        self.dropped_image_index: int | None = None
        self.repaired_image_indexes: list[int] = []
        self.proof_drop_armed = False
        self.proof_test_active = False
        self.proof_drop_match: tuple[str, str, int] | None = None
        self.dropped_proof_fragment = False
        self.dropped_proof_index: int | None = None
        self.synthetic_proof: bytes | None = None
        self.expected_proof: bytes | None = None
        self.proof_expected_signed = False
        self.proof_fragment_count = 0
        self.proof_page_requests = 0
        self.proof_history_rx_baseline: int | None = None
        self.proof_page_rx_baseline: int | None = None
        self.history_drop_armed = False
        self.history_drop_match: tuple[str, str] | None = None
        self.history_manifest_retry_seen = False
        self.history_data_after_manifest_retry = 0
        self.backoff_rng = {
            name: random.Random(seed ^ (0x9E3779B1 * (index + 1)))
            for index, name in enumerate(STATIONS)
        }
        self.loss_rng = {
            (source, target): random.Random(
                seed ^ (0x85EBCA77 * (source_index + 1)) ^
                (0xC2B2AE3D * (target_index + 1)))
            for source_index, source in enumerate(STATIONS)
            for target_index, target in enumerate(STATIONS) if source != target
        }
        self.content_rng = random.Random(seed ^ 0xC0FFEE)
        self.rng_lock = threading.Lock()
        self.lock = threading.RLock()
        self.delivery_lock = threading.RLock()
        self.delivery_timers: set[threading.Timer] = set()
        self.stopping = False
        self.nodes: dict[str, SimRadio] = {}
        self.active: list[Transmission] = []
        self.neighbors = ({"A": {"B", "C"}, "B": {"A", "C"}, "C": {"A", "B"}}
                          if topology == "full" else
                          {"A": {"B", "C"}, "B": {"A", "C"}, "C": {"B"}}
                          if topology == "asymmetric" else
                          {"A": {"B"}, "B": {"A", "C"}, "C": {"B"}})
        self.stats = {name: {"frames": 0, "records": 0, "bytes": 0, "repairs": 0,
                             "message_bundles": 0,
                             "image_fragments": 0,
                             "image_repair_requests": 0,
                             "proof_fragments": 0,
                             "proof_fragments_received": 0,
                             "message_fragments_received": 0,
                             "addressed_echoes": 0,
                             "history_data_received": 0,
                             "foreign_message_transmissions": 0,
                             "addressed_data_received": 0,
                             "addressed_page_ends_received": 0,
                             "lost": 0, "burst_losses": 0,
                             "collisions": 0, "airtime": 0.0}
                      for name in STATIONS}
        self.sync_query_counts: dict[tuple, int] = {}
        self.started = time.monotonic()
        self.print_lock = threading.Lock()

    def log(self, message: str) -> None:
        with self.print_lock:
            print(message, flush=True)

    def attach(self, radio: "SimRadio") -> None:
        self.nodes[radio.name] = radio

    def detach(self, name: str, radio: "SimRadio") -> None:
        # Wait out any callback before the owner's SQLite connection closes.
        with self.delivery_lock:
            with self.lock:
                if self.nodes.get(name) is radio:
                    self.nodes.pop(name, None)

    def _drop_for_loss(self, source: str, target: str) -> tuple[bool, bool]:
        """Return (dropped, burst_state_drop) for one directed radio link."""
        link = (source, target)
        rng = self.loss_rng[link]
        if self.loss_model == "independent":
            return rng.random() < self.loss, False
        if self.loss >= 1:
            return True, True
        if link in self.bad_links:
            if rng.random() < self.burst_recovery:
                self.bad_links.discard(link)
            return True, True
        if rng.random() < self.burst_enter_probability:
            self.bad_links.add(link)
        return False, False

    def _deliver(self, name: str, wire: bytes) -> None:
        timer = threading.current_thread()
        with self.delivery_lock:
            with self.lock:
                self.delivery_timers.discard(timer)
                if self.stopping:
                    return
                radio = self.nodes.get(name)
            if radio:
                radio.receive(wire)

    def close(self) -> None:
        # Serialize shutdown against receive callbacks before Chat closes the
        # SQLite stores those callbacks use.
        with self.delivery_lock:
            with self.lock:
                self.stopping = True
                timers = list(self.delivery_timers)
                self.delivery_timers.clear()
            for timer in timers:
                timer.cancel()
        for timer in timers:
            if timer is not threading.current_thread():
                timer.join(timeout=1)

    def note_repair(self, station: str) -> None:
        with self.lock:
            self.stats[station]["repairs"] += 1

    def random_text(self, length: int) -> str:
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        with self.rng_lock:
            return "".join(self.content_rng.choice(alphabet) for _ in range(length))

    def trace(self, direction: str, station: str, record: dict) -> None:
        kind = record.get("t")
        if (direction == "TX" and station == "C" and kind == "q" and
                record.get("j") and not record.get("v") and
                self.delay_first_history_page_end > 0 and
                self.history_drop_match and
                record.get("o") == self.history_drop_match[1]):
            with self.lock:
                self.delayed_history_retry_seen = True
        if (direction == "TX" and station == "B" and kind == "d" and
                self.delayed_history_retry_seen and self.history_drop_match and
                record.get("o") == self.history_drop_match[1]):
            with self.lock:
                self.history_data_after_delayed_retry += 1
        if (direction == "TX" and station == "C" and kind == "q" and
                record.get("j") and not record.get("v") and
                self.history_drop_match and
                record.get("o") == self.history_drop_match[1]):
            with self.lock:
                self.history_manifest_retry_seen = True
        if (direction == "TX" and station == "B" and kind == "d" and
                self.history_manifest_retry_seen and self.history_drop_match and
                record.get("o") == self.history_drop_match[1]):
            with self.lock:
                self.history_data_after_manifest_retry += 1
        if (direction == "TX" and station == "C" and kind == "g" and
                isinstance(record.get("i"), list)):
            with self.lock:
                self.stats[station]["image_repair_requests"] += 1
                self.repaired_image_indexes.extend(
                    index for index in record["i"] if type(index) is int)
        if direction == "TX" and kind == "p":
            with self.lock:
                self.stats[station]["proof_fragments"] += 1
        if direction == "TX" and station == "C" and kind == "q" and record.get("v"):
            with self.lock:
                self.proof_page_requests += 1
                self.proof_history_rx_baseline = self.stats[station]["history_data_received"]
                if self.proof_test_active:
                    self.proof_page_rx_baseline = self.stats[station]["proof_fragments_received"]
        if kind == "q":
            if direction == "TX":
                query_key = (station, record.get("to"), record.get("o"),
                             record.get("a"), record.get("b"),
                             tuple(record.get("r", [])))
                self.sync_query_counts[query_key] = (
                    self.sync_query_counts.get(query_key, 0) + 1)
        if not self.trace_sync or kind not in {"i", "w", "q", "e", "d", "p", "a", "g", "x", "y"}:
            return
        if kind == "q":
            detail = (f"from={record.get('from', '')[:6]} to={record.get('to', '')[:6]} "
                      f"o={record.get('o', '')[:6]} "
                      f"proof={bool(record.get('v'))} "
                      f"after={record.get('a')} through={record.get('b')} "
                      f"window={record.get('f', record.get('a'))}.."
                      f"{record.get('u', record.get('b'))} "
                      f"repair={record.get('r', [])} fragments={record.get('h', [])} "
                      f"end-only={bool(record.get('j'))} attempt={record.get('k', 0)}")
        elif kind == "e":
            detail = (f"from={record.get('from', '')[:6]} to={record.get('to', '')[:6]} "
                      f"o={record.get('o', '')[:6]} "
                      f"after={record.get('a')} last={record.get('b')} "
                      f"seqs={record.get('g', [])}")
        elif kind == "i":
            detail = f"peer={record.get('p', '')[:6]} origins={record.get('v', [])}"
        elif kind == "d":
            detail = (f"to={record.get('to', '')[:6]} o={record.get('o', '')[:6]} "
                      f"s={record.get('s')} part={record.get('i')}/{record.get('z')} "
                      f"ttl={record.get('ttl', 0)}")
        elif kind == "x":
            detail = (f"to={record.get('to', '')[:6]} o={record.get('o', '')[:6]} "
                      f"s={record.get('s')} chunk={record.get('i')}/{record.get('z')} "
                      f"id={record.get('id')} ttl={record.get('ttl')}")
        elif kind == "p":
            detail = (f"o={record.get('o', '')[:6]} s={record.get('s')} "
                      f"part={record.get('i')}/{record.get('z')} "
                      f"ttl={record.get('ttl', 0)}")
        elif kind in {"a", "g", "y"}:
            detail = (f"to={record.get('to', record.get('p', ''))[:6]} "
                      f"o={record.get('o', '')[:6]} s={record.get('s')} "
                      f"id={record.get('id', '')}")
        else:
            detail = f"peer={record.get('p', '')[:6]} ttl={record.get('ttl')}"
        self.log(f"[mesh] {station} {direction} {kind}: {detail}")

    def _airtime(self, payload_len: int) -> float:
        # Approximate KISS/AX.25 framing, bit stuffing, and radio turnaround.
        return ((payload_len + self.framing_bytes) * 8 * 1.2 / self.bitrate +
                self.interframe)

    def transmit(self, radio: "SimRadio", records: tuple[bytes, ...], wire: bytes,
                 airtime_bytes: int) -> None:
        duration = self._airtime(airtime_bytes)
        while True:
            backoff = self.backoff_rng[radio.name].random() * self.contention_window
            time.sleep(backoff * self.time_scale)
            with self.lock:
                if self.stopping:
                    return
                now = time.monotonic()
                heard = set(self.neighbors[radio.name])
                busy_until = max((other.start + other.duration for other in self.active
                                  if other.start + other.duration > now and
                                  other.station in heard), default=now)
                if busy_until > now:
                    continue
                tx = Transmission(radio.name, now, duration * self.time_scale,
                                  wire, records)
                # Hidden stations can start together; only receivers that hear
                # both overlapping transmissions lose those frames.
                for other in self.active:
                    if other.start + other.duration <= now:
                        continue
                    shared_listeners = heard & set(self.neighbors[other.station])
                    for listener in shared_listeners:
                        tx.blocked.add(listener)
                        other.blocked.add(listener)
                    if radio.name in self.neighbors[other.station]:
                        other.blocked.add(radio.name)
                    if other.station in heard:
                        tx.blocked.add(other.station)
                self.active.append(tx)
                stat = self.stats[radio.name]
                stat["frames"] += 1
                stat["records"] += len(records)
                stat["bytes"] += len(wire)
                stat["airtime"] += duration
                bundled_messages = {
                    (record.get("o"), record.get("s"))
                    for raw_record in records
                    if (record := unpack(raw_record)) and record.get("t") == "d"
                }
                if len(bundled_messages) > 1:
                    stat["message_bundles"] += 1
                stat["image_fragments"] += sum(
                    1 for raw_record in records
                    if (record := unpack(raw_record)) and record.get("t") == "x")
                break
        time.sleep(max(0, duration * self.time_scale))
        with self.lock:
            self.active = [item for item in self.active if item is not tx]
            if self.stopping:
                return
            listeners = set(self.neighbors[radio.name])
        drop_indices: set[int] = set()
        targeted_drop_indices: dict[str, set[int]] = {}
        if self.drop_first_history_fragment and radio.name == "B" and self.history_drop_match:
            index = next((i for i, item in enumerate(records)
                          if (record := unpack(item)) and record.get("t") == "d" and
                          record.get("o") == self.history_drop_match[1]), None)
            with self.lock:
                if (index is not None and self.history_drop_armed and
                        not self.dropped_history_fragment):
                    self.dropped_history_fragment = True
                    drop_indices.add(index)
                    self.history_drop_armed = False
                    self.log("[mesh] deterministically dropped one A-origin history fragment")
        if self.drop_first_history_page_end and self.history_drop_match:
            index = next((i for i, item in enumerate(records)
                          if (record := unpack(item)) and record.get("t") == "e" and
                          record.get("to") == self.history_drop_match[0] and
                          record.get("o") == self.history_drop_match[1]), None)
            with self.lock:
                if (index is not None and self.history_drop_armed and
                        not self.dropped_history_page_end):
                    self.dropped_history_page_end = True
                    drop_indices.add(index)
                    self.history_drop_armed = False
                    self.log("[mesh] deterministically dropped one addressed history page-end")
        if self.drop_first_image_fragment and radio.name == "B" and self.image_drop_match:
            recipient_station, origin_station, wanted_seq = self.image_drop_match
            recipient = self.nodes.get(recipient_station)
            index = next((i for i, item in enumerate(records)
                          if (record := unpack(item)) and record.get("t") == "x" and
                          record.get("to") == getattr(recipient, "node_id", None) and
                          record.get("o") == origin_station and
                          int(record.get("s", 0)) == wanted_seq), None)
            with self.lock:
                if index is not None and self.image_drop_armed and not self.dropped_image_fragment:
                    dropped = unpack(records[index])
                    self.dropped_image_fragment = True
                    self.image_drop_armed = False
                    self.dropped_image_index = int(dropped["i"])
                    targeted_drop_indices.setdefault(recipient_station, set()).add(index)
                    self.log(f"[mesh] deterministically dropped requested image chunk "
                             f"{self.dropped_image_index}")
        if self.drop_first_proof_fragment and radio.name == "B" and self.proof_drop_match:
            recipient_station, origin_station, wanted_seq = self.proof_drop_match
            index = next((i for i, item in enumerate(records)
                          if (record := unpack(item)) and record.get("t") == "p" and
                          record.get("o") == origin_station and
                          int(record.get("s", 0)) == wanted_seq), None)
            with self.lock:
                if index is not None and self.proof_drop_armed and not self.dropped_proof_fragment:
                    dropped = unpack(records[index])
                    self.dropped_proof_fragment = True
                    self.proof_drop_armed = False
                    self.dropped_proof_index = int(dropped["i"])
                    targeted_drop_indices.setdefault(recipient_station, set()).add(index)
                    self.log(f"[mesh] deterministically dropped proof chunk "
                             f"{self.dropped_proof_index} on B→{recipient_station}")
        for name in sorted(listeners):
            if name in tx.blocked:
                with self.lock:
                    self.stats[radio.name]["collisions"] += 1
                    self.stats[radio.name]["lost"] += 1
                continue
            dropped, burst_drop = self._drop_for_loss(radio.name, name)
            if dropped:
                with self.lock:
                    self.stats[radio.name]["lost"] += 1
                    if burst_drop:
                        self.stats[radio.name]["burst_losses"] += 1
                continue
            visible_records = tuple(record for index, record in enumerate(records)
                                    if index not in drop_indices and
                                    index not in targeted_drop_indices.get(name, set()))
            if not visible_records:
                with self.lock:
                    self.stats[radio.name]["lost"] += 1
                continue
            delayed_records: list[bytes] = []
            if (radio.name == "B" and name == "C" and
                    self.delay_first_history_page_end > 0 and
                    self.delay_history_page_end_armed and self.history_drop_match):
                receiver = self.nodes.get(name)
                target_id = receiver.node_id if receiver else ""
                matching_end = False
                contains_matching_data = False
                for raw_record in visible_records:
                    record = unpack(raw_record)
                    if not record:
                        continue
                    if (record.get("t") == "e" and record.get("to") == target_id and
                            record.get("o") == self.history_drop_match[1]):
                        matching_end = True
                    if (record.get("t") == "d" and
                            record.get("o") == self.history_drop_match[1]):
                        contains_matching_data = True
                with self.lock:
                    if (not self.delayed_history_page_end and matching_end and
                            not contains_matching_data):
                        self.delayed_history_page_end = True
                        self.delayed_history_page_end_count += 1
                        self.delay_history_page_end_armed = False
                        delayed_records = list(visible_records)
                if delayed_records:
                    self.log("[mesh] delaying first B-to-C history page-end by "
                             f"{self.delay_first_history_page_end:.2f}s")
                immediate_records = ([] if delayed_records else list(visible_records))
            else:
                immediate_records = list(visible_records)

            deliveries = []
            if immediate_records:
                immediate_wire, _ = radio.encode_wire(tuple(immediate_records))
                deliveries.append((immediate_wire, 0.0))
            if delayed_records:
                delayed_wire, _ = radio.encode_wire(tuple(delayed_records))
                deliveries.append((delayed_wire, self.delay_first_history_page_end))
            for delivered_wire, extra_delay in deliveries:
                delay = ((self.propagation + extra_delay) * self.time_scale)
                timer = threading.Timer(delay, self._deliver,
                                        args=(name, delivered_wire))
                with self.lock:
                    if self.stopping:
                        continue
                    self.delivery_timers.add(timer)
                    timer.start()

    def report(self) -> None:
        elapsed = time.monotonic() - self.started
        with self.lock:
            rows = [(name, dict(values)) for name, values in self.stats.items()]
        print(f"wall elapsed={elapsed:.1f}s bitrate={self.bitrate}bps "
              f"loss={self.loss:.0%} model={self.loss_model} "
              f"propagation={self.propagation:.3f}s",
              flush=True)
        for name, stat in rows:
            print(f"{name}: {stat['frames']} frames, {stat['records']} records, "
                  f"{stat['bytes']} bytes, airtime={stat['airtime']:.2f}s, "
                  f"losses={stat['lost']} collisions={stat['collisions']} "
                  f"message-bundles={stat['message_bundles']} "
                  f"image-fragments={stat['image_fragments']} "
                  f"image-repair-requests={stat['image_repair_requests']} "
                  f"proof-fragments={stat['proof_fragments']}/"
                  f"{stat['proof_fragments_received']} "
                  f"message-rx={stat['message_fragments_received']} "
                  f"addressed-echoes={stat['addressed_echoes']} "
                  f"history-rx={stat['history_data_received']} "
                  f"foreign-message-tx={stat['foreign_message_transmissions']} "
                  f"addressed-rx={stat['addressed_data_received']}/"
                  f"{stat['addressed_page_ends_received']} "
                  f"page-repair-requests={stat['repairs']}", flush=True)


class SimRadio:
    """Small Data2G-shaped adapter around Chat's actual on_frame callback."""
    port = 0

    def __init__(self, name: str, node_id: str, mesh: Mesh, batch_window: float):
        self.name = name
        self.node_id = node_id
        self.mesh = mesh
        self.transport = mesh.transport
        self.batch_window = batch_window * mesh.time_scale
        self.pending: dict[int, deque[bytes]] = {0: deque(), 1: deque()}
        self.condition = threading.Condition()
        self.processing = 0
        self.stopping = False
        self.on_frame = None
        self.worker = threading.Thread(target=self._write, name=f"sim-radio-{name}",
                                       daemon=True)
        mesh.attach(self)

    def start(self) -> None:
        self.worker.start()

    def send(self, payload: bytes, *, priority: int = 0) -> None:
        record = unpack(payload)
        if record:
            self.mesh.trace("TX", self.name, record)
            if record.get("t") == "d":
                if record.get("o") != self.node_id:
                    with self.mesh.lock:
                        self.mesh.stats[self.name]["foreign_message_transmissions"] += 1
            if (record.get("t") in {"d", "e"} and
                    record.get("to") == self.node_id):
                with self.mesh.lock:
                    self.mesh.stats[self.name]["addressed_echoes"] += 1
        if record and record.get("t") == "q" and record.get("r"):
            self.mesh.note_repair(self.name)
        with self.condition:
            if self.stopping:
                raise ConnectionError("simulated radio is closed")
            self.pending[int(priority > 0)].append(bytes(payload))
            self.condition.notify()

    def _write(self) -> None:
        while True:
            with self.condition:
                while not self.stopping and not any(self.pending.values()):
                    self.condition.wait()
                if self.stopping:
                    return
                priority = 0 if self.pending[0] else 1
                first = self.pending[priority].popleft()
                self.processing += 1
            if self.batch_window:
                time.sleep(self.batch_window)
            try:
                records = [first]
                with self.condition:
                    pending = self.pending[priority]
                    while pending:
                        candidate = records + [pending[0]]
                        bundled = pack_batch(candidate, max_bytes=self._batch_limit())
                        if bundled is None:
                            break
                        records.append(pending.popleft())
                wire, airtime_bytes = self.encode_wire(tuple(records))
                self.mesh.transmit(self, tuple(records), wire, airtime_bytes)
            finally:
                with self.condition:
                    self.processing -= 1
                    self.condition.notify_all()

    def _batch_limit(self) -> int:
        return 256 if self.transport == "ax25" else 220

    def encode_wire(self, records: tuple[bytes, ...]) -> tuple[bytes, int]:
        info = pack_batch(records, max_bytes=self._batch_limit()) or records[0]
        if self.transport == "ax25":
            frame = encode_ui_frame(CALLSIGNS[self.name], "CQ", (), info)
            # KISS bytes go over the simulated TCP link; only the AX.25 UI
            # frame contributes payload bytes to the radio airtime estimate.
            return encode_kiss(0, KISS_DATA, frame), len(frame)
        return info, len(info)

    def receive(self, payload: bytes) -> None:
        if self.on_frame:
            if self.transport == "ax25":
                parser = KissDecoder()
                frames = parser.feed(payload)
                records = tuple(
                    record
                    for port, command, kiss_payload in frames
                    if port == 0 and command == KISS_DATA
                    if (info := decode_ui_frame(kiss_payload)) is not None
                    for record in (unpack_batch(info) or (info,)))
            else:
                records = unpack_batch(payload) or (payload,)
            for record in records:
                decoded = unpack(record)
                if decoded:
                    self.mesh.trace("RX", self.name, decoded)
                    if decoded and decoded.get("t") == "d":
                        with self.mesh.lock:
                            self.mesh.stats[self.name]["message_fragments_received"] += 1
                    if decoded and decoded.get("t") == "p":
                        with self.mesh.lock:
                            self.mesh.stats[self.name]["proof_fragments_received"] += 1
                    if (decoded.get("t") == "d" and
                            decoded.get("o") != self.node_id):
                        with self.mesh.lock:
                            self.mesh.stats[self.name]["history_data_received"] += 1
                    if decoded.get("to") == self.node_id:
                        counter = {"d": "addressed_data_received",
                                   "e": "addressed_page_ends_received"}.get(
                                       decoded.get("t"))
                        if counter:
                            with self.mesh.lock:
                                self.mesh.stats[self.name][counter] += 1
                self.on_frame(self.port, record)

    def close(self) -> None:
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
        if self.worker.is_alive():
            self.worker.join(timeout=2)


def make_args(name: str, nick: str, db: str, transport: str = "broadcast",
              sync_retry_seconds: float = 12.0) -> SimpleNamespace:
    auth_dir = os.environ.get("KISSCHAT_SIM_ARAP_DIR", "")
    auth_root = Path(auth_dir) if auth_dir else None
    return SimpleNamespace(
        host="simulated-radio", callsign=CALLSIGNS[name], nick=nick,
        channel="#general", db=db, mode="", transport=transport,
        command_port=8300, kiss_port=8100, bandwidth="2300",
        ax25_destination="CQ", digipeaters="", kiss_tnc_port=0,
        ax25_baud=1200, ax25_interframe_delay=0.25,
        arap_cli=os.environ.get("KISSCHAT_SIM_ARAP_CLI", "arap"),
        arap_key=(str(auth_root / "private" / f"{CALLSIGNS[name]}.age")
                  if auth_root else ""),
        arap_key_dir=str(auth_root / "public") if auth_root else "",
        arap_trust_store=str(auth_root / "trust.json") if auth_root else "",
        arap_passphrase_env="KISSCHAT_ARAP_PASSPHRASE",
        headless=False, auto_sync=False, sync_retry_seconds=sync_retry_seconds,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loss", type=float, default=0.10,
                        help="target per-link receive loss from 0 to 1 (default 0.10)")
    parser.add_argument("--loss-model", choices=("independent", "bursty"),
                        default="independent",
                        help="independent packet drops or correlated Gilbert-Elliott bursts")
    parser.add_argument("--burst-recovery", type=float, default=0.5,
                        help="per-frame probability a bad burst link recovers (default 0.5)")
    parser.add_argument("--bitrate", type=int, default=1200,
                        help="simulated radio bit rate (default 1200)")
    parser.add_argument("--propagation", type=float, default=0.05,
                        help="one-way propagation delay in seconds (default 0.05)")
    parser.add_argument("--interframe", type=float, default=0.25,
                        help="radio turnaround/interframe delay in seconds (default 0.25)")
    parser.add_argument("--framing-bytes", type=int, default=24,
                        help="estimated framing/preamble bytes added to airtime (default 24)")
    parser.add_argument("--contention-window", type=float, default=0.5,
                        help="maximum random channel-access backoff in seconds (default 0.5)")
    parser.add_argument("--batch-window", type=float, default=0.08,
                        help="seconds to collect queued records for a compressed frame")
    parser.add_argument("--time-scale", type=float, default=1.0,
                        help="wall-time scale; 0.1 runs the radio model 10x faster")
    parser.add_argument("--seed", type=int, default=7,
                        help="random seed for repeatable packet loss")
    parser.add_argument("--sync-retry-seconds", type=float, default=12.0,
                        help="simulated-client sync retry interval (default 12)")
    parser.add_argument("--topology", choices=("full", "line", "asymmetric"), default="full",
                        help="full mesh, A-B-C line, or one-way A-to-C hearing")
    parser.add_argument("--transport", choices=("broadcast", "ax25"), default="broadcast",
                        help="simulate Data2G payloads or raw KISS with AX.25 UI frames")
    parser.add_argument("--script", type=Path,
                        help="read simulator commands from a file, then exit")
    parser.add_argument("--json-report", type=Path,
                        help="write machine-readable scenario results and station histories")
    parser.add_argument("--trace-sync", action="store_true",
                        help="log inventory, page-request, and page-end records")
    parser.add_argument("--drop-first-history-fragment", action="store_true",
                        help="drop one B-sent A-origin history fragment during sync C A")
    parser.add_argument("--drop-first-history-page-end", action="store_true",
                        help="drop one B-to-C history page-end during targeted sync C A")
    parser.add_argument("--delay-first-history-page-end", type=float, default=0.0,
                        help="delay the first targeted B-to-C history page-end by seconds")
    parser.add_argument("--drop-first-image-fragment", action="store_true",
                        help="drop one B-to-C image chunk after explicit request-image C A")
    parser.add_argument("--drop-first-proof-fragment", action="store_true",
                        help="drop one B-to-C detached proof chunk during targeted sync C A")
    opts = parser.parse_args()
    if (not 0 <= opts.loss <= 1 or not 0 < opts.burst_recovery <= 1 or
            opts.bitrate <= 0 or opts.propagation < 0 or
            opts.interframe < 0 or opts.framing_bytes < 0 or
            opts.contention_window < 0 or opts.batch_window < 0 or
            opts.delay_first_history_page_end < 0 or
            opts.sync_retry_seconds <= 0 or
            not 0 < opts.time_scale <= 10):
        parser.error("loss must be 0..1, bitrate positive, delays nonnegative, "
                     "and time-scale in (0, 10]")

    temp = tempfile.TemporaryDirectory(prefix="kisschat-mesh-")
    mesh = Mesh(loss=opts.loss, bitrate=opts.bitrate, propagation=opts.propagation,
                interframe=opts.interframe, framing_bytes=opts.framing_bytes,
                contention_window=opts.contention_window,
                time_scale=opts.time_scale, seed=opts.seed, topology=opts.topology,
                loss_model=opts.loss_model, burst_recovery=opts.burst_recovery,
                transport=opts.transport,
                trace_sync=opts.trace_sync,
                drop_first_history_fragment=opts.drop_first_history_fragment,
                drop_first_history_page_end=opts.drop_first_history_page_end,
                delay_first_history_page_end=opts.delay_first_history_page_end,
                drop_first_image_fragment=opts.drop_first_image_fragment,
                drop_first_proof_fragment=opts.drop_first_proof_fragment)
    chats: dict[str, Chat] = {}
    radios: dict[str, SimRadio] = {}
    try:
        for name, nick in zip(STATIONS, ("Alice", "Bob", "Casey")):
            chat = Chat(make_args(name, nick, os.path.join(temp.name, name + ".sqlite3"),
                                  opts.transport, opts.sync_retry_seconds))
            radio = SimRadio(name, chat.store.node, mesh, opts.batch_window)
            radio.on_frame = chat._on_frame
            chat.data = radio
            chats[name] = chat
            radios[name] = radio
        for chat in chats.values():
            chat.start()

        stop = threading.Event()

        def show_events() -> None:
            while not stop.is_set():
                found = False
                for name, chat in chats.items():
                    try:
                        event, value = chat.events.get_nowait()
                    except queue.Empty:
                        continue
                    found = True
                    if event == "message":
                        mesh.log(f"[{name}] {value['c']} <{value['n']}> {value['b']}")
                    elif event in {"status", "receipt"}:
                        mesh.log(f"[{name}] {value}")
                if not found:
                    stop.wait(0.03)

        printer = threading.Thread(target=show_events, name="sim-events", daemon=True)
        printer.start()
        mesh.log(f"KISSChat 3-station {opts.transport} simulator; no modem or RF used")
        mesh.log("Station IDs: " + " ".join(
            f"{name}={chats[name].store.node[:6]}" for name in STATIONS))
        if opts.topology == "line":
            mesh.log("Heard stations: A hears B only; B hears A and C; C hears B only")
        elif opts.topology == "asymmetric":
            mesh.log("Heard stations: A hears B; B hears A and C; C hears A and B; "
                     "A cannot hear C")
        else:
            mesh.log("Heard stations: A, B, and C are all in range of one another")
        mesh.log("Commands: send A message | burst A COUNT message | sync A|C [origin A|C] | install-proof A | verify-proof-isolated | verify-signed-isolated B A | verify-proof C A | verify-delayed-page-end | stop-station A | start-station A | verify-offline A | verify-restarted A | wait-radio-idle | verify-hearing RX TX | verify-deaf RX TX | partition A B | rejoin B C | verify-partition A B | forget C A | send-image A caption | request-image C A | retry-image C | history C | verify | verify-quiet | verify-bundling | verify-no-echo | stats | wait SECONDS | quit")
        verification_failed = False
        checks_run: list[str] = []
        history_window_probes: dict[str, list[tuple[int, bool]]] = {}
        partition_baselines: dict[tuple[str, str], dict[str, int]] = {}
        partitioned_links: set[tuple[str, str]] = set()
        image_payload = BytesIO()
        Image.new("RGB", (160, 120), (54, 122, 156)).save(
            image_payload, format="JPEG", quality=70, optimize=True)
        image_bytes = image_payload.getvalue()
        published_image: dict[str, object] = {}
        sent_messages: dict[str, tuple[str, int]] = {}
        signed_verifications: list[dict] = []
        station_nicks = dict(zip(STATIONS, ("Alice", "Bob", "Casey")))
        station_db_paths = {name: os.path.join(temp.name, name + ".sqlite3")
                            for name in STATIONS}
        stopped_stations: set[str] = set()
        stopped_once: set[str] = set()
        station_restarts = {name: 0 for name in STATIONS}
        station_node_ids = {name: chat.store.node for name, chat in chats.items()}
        station_highwater = {name: chat.store.highwater(chat.store.node)
                             for name, chat in chats.items()}
        offline_origin_syncs: list[str] = []

        def history_for_station(name: str, limit: int) -> list[dict]:
            if name not in stopped_stations:
                return chats[name].store.history(chats[name].channel, limit)
            snapshot = Store(station_db_paths[name], CALLSIGNS[name])
            try:
                return snapshot.history("#general", limit)
            finally:
                snapshot.close()

        def execute(line: str) -> bool:
            nonlocal verification_failed
            line = line.strip()
            if not line or line.startswith("#"):
                return True
            command, _, rest = line.partition(" ")
            command = command.lower()
            if command == "verify" or command.startswith("verify-"):
                checks_run.append(command)
            if command in {"quit", "exit"}:
                return False
            if command == "send":
                station, _, body = rest.partition(" ")
                station = station.upper()
                if body.startswith("random "):
                    try:
                        length = int(body.partition(" ")[2])
                    except ValueError:
                        length = 0
                    if not 1 <= length <= 3000:
                        mesh.log("usage: send A random BYTES (1..3000)")
                        return True
                    body = mesh.random_text(length)
                if station not in chats or not body:
                    mesh.log("usage: send A message text")
                else:
                    chats[station].publish(body)
                    sent_messages[station] = (chats[station].store.node,
                                              chats[station].store.highwater(
                                                  chats[station].store.node))
                    station_highwater[station] = chats[station].store.highwater(
                        chats[station].store.node)
                return True
            if command == "install-proof":
                station = rest.strip().upper()
                identity = sent_messages.get(station)
                if station not in chats or not identity:
                    mesh.log("usage: install-proof A (after sending a message)")
                    verification_failed = True
                else:
                    origin, seq = identity
                    proof = bytes((index * 37 + 11) % 256 for index in range(760))
                    chats[station].store.set_auth(origin, seq, proof, "pending")
                    mesh.synthetic_proof = proof
                    mesh.expected_proof = proof
                    mesh.proof_fragment_count = (len(proof) + 189) // 190
                    mesh.proof_drop_match = ("C", origin, seq)
                    chats[station]._send_proof(origin, seq, proof)
                    mesh.log(f"[{station}] installed deterministic opaque proof fixture "
                             f"for {origin[:6]}:{seq} and transmitted it")
                return True
            if command == "verify-signed":
                receiver, _, origin_station = rest.partition(" ")
                receiver, origin_station = receiver.upper(), origin_station.strip().upper()
                identity = sent_messages.get(origin_station)
                if receiver not in chats or origin_station not in chats or not identity:
                    verification_failed = True
                    mesh.log("usage: verify-signed B|C A (after send A with ARAP enabled)")
                    return True
                origin, seq = identity
                states = {name: chats[name].store.auth_for(origin, seq)
                          for name in (origin_station, "B", "C")}
                proofs = {name: chats[name].store.proof_for(origin, seq)
                          for name in (origin_station, "B", "C")}
                proof_values = [proof for proof in proofs.values() if proof]
                same_proof = (len(proof_values) == 3 and
                              len(set(proof_values)) == 1)
                expected_signer = CALLSIGNS[origin_station]
                trusted = all(state["status"] == "verified" and
                              state["signer"] == expected_signer
                              for state in states.values())
                passed = same_proof and trusted
                signed_verifications.append({
                    "origin_station": origin_station, "receiver": receiver,
                    "origin": origin, "sequence": seq,
                    "proof_bytes": len(proof_values[0]) if proof_values else 0,
                    "byte_identical_at_all_stations": same_proof,
                    "verified_statuses": {name: state["status"]
                                          for name, state in states.items()},
                })
                if passed:
                    mesh.log("ARAP RELAY PASS: trusted signature verified at A/B/C; "
                             "detached proof bytes are identical")
                else:
                    verification_failed = True
                    status_summary = ", ".join(
                        f"{name}:{state['status']}" for name, state in states.items())
                    mesh.log("ARAP RELAY FAIL: "
                             f"proofs-identical={same_proof}, "
                             f"statuses={{{status_summary}}}")
                return True
            if command == "verify-signed-isolated":
                relay, _, origin_station = rest.partition(" ")
                relay, origin_station = relay.upper(), origin_station.strip().upper()
                identity = sent_messages.get(origin_station)
                if relay not in chats or origin_station not in chats or not identity:
                    verification_failed = True
                    mesh.log("usage: verify-signed-isolated B A (after ARAP signing)")
                    return True
                origin, seq = identity
                proof = chats[relay].store.proof_for(origin, seq)
                state = chats[relay].store.auth_for(origin, seq)
                receiver_isolated = (
                    not chats["C"].store.has_message(origin, seq) and
                    chats["C"].store.proof_for(origin, seq) is None)
                passed = (relay == "B" and bool(proof) and
                          state["status"] == "verified" and
                          state["signer"] == CALLSIGNS[origin_station] and
                          receiver_isolated)
                if passed:
                    mesh.expected_proof = proof
                    mesh.proof_expected_signed = True
                    mesh.proof_fragment_count = (len(proof) + 189) // 190
                    mesh.proof_drop_match = ("C", origin, seq)
                    mesh.log("SIGNED PROOF CACHE PASS: B has a trusted complete proof; "
                             "C has neither body nor proof")
                else:
                    verification_failed = True
                    mesh.log("SIGNED PROOF CACHE FAIL: "
                             f"B-status={state['status']}, B-proof={bool(proof)}, "
                             f"C-isolated={receiver_isolated}")
                return True
            if command == "verify-offline":
                station = rest.strip().upper()
                offline = (station in stopped_stations and
                           station not in mesh.nodes and
                           (not chats[station].sync_thread or
                            not chats[station].sync_thread.is_alive())) if station in chats else False
                if offline:
                    mesh.log(f"OFFLINE PASS: {station} process and radio receiver are stopped")
                else:
                    verification_failed = True
                    mesh.log(f"OFFLINE FAIL: {station} process or radio is still active")
                return True
            if command == "verify-restarted":
                station = rest.strip().upper()
                original_node = station_node_ids.get(station)
                durable = (station in chats and station not in stopped_stations and
                           bool(original_node) and chats[station].store.node == original_node)
                identity = sent_messages.get(station)
                if identity and durable:
                    durable = chats[station].store.has_message(*identity)
                    if durable and mesh.proof_expected_signed:
                        auth = chats[station].store.auth_for(*identity)
                        durable = (auth["status"] == "verified" and
                                   auth["signer"] == CALLSIGNS[station])
                if durable and station_restarts.get(station, 0) > 0:
                    retained = ("identity and verified proof" if mesh.proof_expected_signed
                                else "identity and message history")
                    mesh.log(f"RESTART PASS: {station} reopened its database and kept "
                             f"{retained}")
                else:
                    verification_failed = True
                    mesh.log(f"RESTART FAIL: {station} identity or signed message did not survive restart")
                return True
            if command == "verify-proof":
                station, _, origin_station = rest.partition(" ")
                station, origin_station = station.upper(), origin_station.upper()
                identity = sent_messages.get(origin_station)
                if (station not in chats or origin_station not in chats or not identity or
                        mesh.expected_proof is None):
                    verification_failed = True
                    mesh.log("usage: verify-proof C A (after install-proof A)")
                    return True
                origin, seq = identity
                proof = chats[station].store.proof_for(origin, seq)
                history_unchanged = (
                    mesh.proof_history_rx_baseline is not None and
                    mesh.stats[station]["history_data_received"] ==
                    mesh.proof_history_rx_baseline)
                proof_page_retransmitted = (
                    mesh.proof_page_rx_baseline is not None and
                    mesh.stats[station]["proof_fragments_received"] >=
                    mesh.proof_page_rx_baseline + mesh.proof_fragment_count)
                auth = chats[station].store.auth_for(origin, seq)
                valid_signature = (not mesh.proof_expected_signed or
                                   (auth["status"] == "verified" and
                                    auth["signer"] == CALLSIGNS[origin_station]))
                passed = (proof == mesh.expected_proof and valid_signature and
                          mesh.dropped_proof_fragment and
                          mesh.proof_page_requests > 0 and history_unchanged and
                          proof_page_retransmitted)
                if passed:
                    mesh.log("PROOF REPAIR PASS: dropped proof fragment recovered byte-for-byte "
                             "without replaying the message body")
                else:
                    verification_failed = True
                    mesh.log("PROOF REPAIR FAIL: "
                             f"proof-match={proof == mesh.expected_proof}, "
                             f"trusted-signature={valid_signature}, "
                             f"dropped={mesh.dropped_proof_fragment}, "
                             f"proof-requests={mesh.proof_page_requests}, "
                             f"body-replay-free={history_unchanged}, "
                             f"proof-page-retransmitted={proof_page_retransmitted}")
                return True
            if command == "verify-proof-isolated":
                identity = sent_messages.get("A")
                if not identity or mesh.synthetic_proof is None:
                    verification_failed = True
                    mesh.log("PROOF SETUP FAIL: install-proof A is required")
                    return True
                origin, seq = identity
                relay_has_proof = chats["B"].store.proof_for(origin, seq) == mesh.synthetic_proof
                receiver_has_no_proof = (
                    chats["C"].store.proof_for(origin, seq) is None and
                    mesh.stats["C"]["proof_fragments_received"] == 0)
                if relay_has_proof and receiver_has_no_proof:
                    mesh.log("PROOF CACHE PASS: B has all proof bytes; partition kept C isolated")
                else:
                    verification_failed = True
                    mesh.log("PROOF CACHE FAIL: "
                             f"B-complete={relay_has_proof}, C-isolated={receiver_has_no_proof}")
                return True
            if command in {"verify-hearing", "verify-deaf"}:
                receiver, _, transmitter = rest.partition(" ")
                receiver, transmitter = receiver.upper(), transmitter.strip().upper()
                identity = sent_messages.get(transmitter)
                if receiver not in chats or transmitter not in chats or not identity:
                    mesh.log(f"usage: {command} RX TX (after TX sends a message)")
                    verification_failed = True
                else:
                    heard = chats[receiver].store.has_message(identity[0], identity[1])
                    passed = heard if command == "verify-hearing" else not heard
                    if not passed:
                        verification_failed = True
                    label = "HEARING PASS" if passed and command == "verify-hearing" else (
                        "DEAFNESS PASS" if passed else "HEARING FAIL")
                    mesh.log(f"{label}: {receiver} " +
                             ("heard" if heard else "did not hear") +
                             f" {transmitter}'s message")
                return True
            if command == "burst":
                station, _, tail = rest.partition(" ")
                station = station.upper()
                count_text, _, body = tail.partition(" ")
                try:
                    count = int(count_text)
                except ValueError:
                    count = 0
                if station not in chats or not 2 <= count <= 32 or not body:
                    mesh.log("usage: burst A COUNT message (2..32)")
                else:
                    for index in range(count):
                        chats[station].publish(f"{body} {index + 1}")
                return True
            if command == "send-aged":
                station, _, tail = rest.partition(" ")
                station = station.upper()
                age_text, _, body = tail.partition(" ")
                try:
                    age_seconds = int(age_text)
                except ValueError:
                    age_seconds = -1
                if station not in chats or not 0 <= age_seconds <= 10 * 365 * 86400 or not body:
                    mesh.log("usage: send-aged A AGE_SECONDS message")
                else:
                    chat = chats[station]
                    timestamp = int(time.time()) - age_seconds
                    message = chat.store.insert_local(chat.channel, chat.nick,
                                                      timestamp, body)
                    for frame in message_parts(
                            origin=message["o"], seq=message["s"],
                            channel=message["c"], nick=message["n"],
                            timestamp=timestamp, text=message["b"]):
                        chat.data.send(frame)
                    chat.announce_origin(message["o"], force=True,
                                         high=int(message["s"]))
                    history_window_probes.setdefault(message["o"], []).append(
                        (int(message["s"]), age_seconds <= 48 * 60 * 60))
                return True
            if command == "send-image":
                station, _, caption = rest.partition(" ")
                station = station.upper()
                if station not in chats or not caption:
                    mesh.log("usage: send-image A caption")
                else:
                    chat = chats[station]
                    chat.publish(caption, thumbnail=(image_bytes, 160, 120))
                    seq = chat.store.highwater(chat.store.node)
                    sent_messages[station] = (chat.store.node, seq)
                    station_highwater[station] = seq
                    attachment = chat.store.attachment_for(chat.store.node, seq)
                    if attachment:
                        published_image.update(origin=chat.store.node, seq=seq,
                                               digest=attachment["digest"])
                return True
            if command == "request-image":
                station, _, origin_station = rest.partition(" ")
                station, origin_station = station.upper(), origin_station.upper()
                attachments = chats[station].store.recent_attachments(chats[station].channel)
                attachment = next((item for item in attachments
                                   if origin_station in chats and
                                   item["origin"] == chats[origin_station].store.node), None)
                if not attachment:
                    mesh.log("IMAGE REQUEST FAIL: no matching attachment metadata is cached")
                    verification_failed = True
                else:
                    chats[station]._request_thumbnail(attachment, attempt=0)
                    published_image.update(origin=attachment["origin"],
                                           seq=attachment["seq"],
                                           digest=attachment["digest"])
                    if (mesh.drop_first_image_fragment and station == "C" and
                            origin_station == "A"):
                        mesh.image_drop_match = (
                            station, attachment["origin"], int(attachment["seq"]))
                        mesh.image_drop_armed = True
                return True
            if command == "retry-image":
                station = rest.strip().upper()
                if station not in chats:
                    mesh.log("usage: retry-image C")
                    verification_failed = True
                else:
                    chat = chats[station]
                    now = time.monotonic()
                    with chat.image_lock:
                        for key, (attempt, _retry_at, expires) in list(
                                chat.pending_images.items()):
                            chat.pending_images[key] = (attempt, now - 1, expires)
                    chat._retry_pending_images()
                    mesh.log(f"[{station}] retried outstanding thumbnail requests")
                return True
            if command == "verify-image-metadata":
                station, _, origin_station = rest.partition(" ")
                station, origin_station = station.upper(), origin_station.upper()
                origin = chats[origin_station].store.node if origin_station in chats else ""
                attachment = chats[station].store.attachment_for(
                    origin, int(published_image.get("seq", 0))) if origin else None
                transmitted = sum(stat["image_fragments"] for stat in mesh.stats.values())
                has_message = (origin and chats[station].store.has_message(
                    origin, int(published_image.get("seq", 0))))
                if (has_message and attachment and
                        attachment["digest"] == published_image.get("digest") and
                        not chats[station].store.image_for(attachment["digest"]) and
                        transmitted == 0):
                    mesh.log("IMAGE PASS: message and metadata synced; no thumbnail bytes sent before request")
                else:
                    verification_failed = True
                    mesh.log("IMAGE FAIL: message/metadata missing or thumbnail bytes sent automatically")
                return True
            if command == "verify-offline-image-cache":
                relay, _, origin_station = rest.partition(" ")
                relay, origin_station = relay.upper(), origin_station.upper()
                origin = (station_node_ids.get(origin_station, "")
                          if origin_station in stopped_stations else "")
                seq = int(published_image.get("seq", 0))
                digest = str(published_image.get("digest", ""))
                cached = chats[relay].store.image_for(digest) if relay in chats else None
                metadata = (chats["C"].store.attachment_for(origin, seq)
                            if origin and "C" in chats else None)
                receiver_empty = (not chats["C"].store.image_for(digest) and
                                  chats["C"].store.has_message(origin, seq)) if metadata else False
                quiet_receiver = mesh.stats["C"]["image_fragments"] == 0
                if (origin and relay != origin_station and cached == image_bytes and
                        metadata and metadata["digest"] == digest and
                        receiver_empty and quiet_receiver):
                    mesh.log(f"OFFLINE IMAGE CACHE PASS: {relay} has the requested bytes; "
                             f"{origin_station} is offline and C has metadata only")
                else:
                    verification_failed = True
                    mesh.log("OFFLINE IMAGE CACHE FAIL: origin/cache/receiver state "
                             "does not prove cached store-and-forward")
                return True
            if command == "verify-image-received":
                station = rest.strip().upper()
                origin = str(published_image.get("origin", ""))
                seq = int(published_image.get("seq", 0))
                digest = str(published_image.get("digest", ""))
                received = chats[station].store.image_for(digest) if station in chats else None
                if received == image_bytes and chats[station].store.attachment_for(origin, seq):
                    if mesh.drop_first_image_fragment:
                        repaired = (mesh.dropped_image_fragment and
                                    mesh.dropped_image_index is not None and
                                    mesh.stats[station]["image_repair_requests"] > 0 and
                                    mesh.repaired_image_indexes == [mesh.dropped_image_index])
                        if not repaired:
                            verification_failed = True
                            mesh.log("IMAGE FAIL: thumbnail completed without the expected "
                                     "single missing-chunk request")
                            return True
                        mesh.log("IMAGE REPAIR PASS: one dropped chunk was requested selectively "
                                 "and the thumbnail digest verified")
                    else:
                        mesh.log("IMAGE PASS: requested thumbnail received and verified by digest")
                else:
                    verification_failed = True
                    progress = (chats[station].store.image_progress(origin, seq)
                                if station in chats else (0, 0))
                    mesh.log(f"IMAGE FAIL: requested thumbnail incomplete; "
                             f"fragments={progress[0]}/{progress[1]}")
                return True
            if command == "verify-history-window":
                station, _, origin_station = rest.partition(" ")
                station, origin_station = station.upper(), origin_station.upper()
                if station not in chats or origin_station not in chats:
                    mesh.log("usage: verify-history-window C A")
                    verification_failed = True
                    return True
                origin = chats[origin_station].store.node
                probes = history_window_probes.get(origin, [])
                checks = [chats[station].store.has_message(origin, seq) == expected
                          for seq, expected in probes]
                if probes and all(checks):
                    mesh.log(f"HISTORY WINDOW PASS: {station} received only the "
                             f"{sum(expected for _, expected in probes)} in-window probe(s)")
                else:
                    verification_failed = True
                    mesh.log("HISTORY WINDOW FAIL: aged history crossed the 48-hour sync window")
                return True
            if command == "verify-receipt":
                station, _, recipient_station = rest.partition(" ")
                station, recipient_station = station.upper(), recipient_station.upper()
                identity = sent_messages.get(station)
                if (not identity or recipient_station not in chats or
                        station not in chats):
                    mesh.log("usage: verify-receipt A C (after sending @K0CCC)")
                    verification_failed = True
                    return True
                origin, seq = identity
                expected = (CALLSIGNS[recipient_station], CALLSIGNS[recipient_station])
                if expected in chats[station].store.receipts_for(origin, seq):
                    mesh.log(f"RECEIPT PASS: {recipient_station} receipt reached {station}")
                else:
                    verification_failed = True
                    mesh.log(f"RECEIPT FAIL: no delivered mention receipt for {recipient_station}")
                return True
            if command == "verify-delayed-page-end":
                origin, seq = sent_messages.get("A", ("", 0))
                query_count = sum(
                    count for key, count in mesh.sync_query_counts.items()
                    if key[0] == "C" and key[1] == chats["B"].store.node and
                    key[2] == origin and not key[5])
                pending = any(key[1] == origin
                              for key in chats["C"].pending_sync_requests)
                passed = (
                    mesh.delayed_history_page_end_count == 1 and
                    mesh.delayed_history_retry_seen and
                    mesh.history_data_after_delayed_retry == 0 and
                    query_count >= 2 and not pending and
                    chats["C"].store.has_message(origin, seq) and
                    mesh.stats["C"]["addressed_page_ends_received"] >= 2)
                if passed:
                    mesh.log("DELAYED PAGE-END PASS: C retried only the manifest, "
                             "received both completions, and stored one message")
                else:
                    verification_failed = True
                    mesh.log("DELAYED PAGE-END FAIL: "
                             f"delayed={mesh.delayed_history_page_end_count}, "
                             f"retry={mesh.delayed_history_retry_seen}, "
                             f"query_count={query_count}, "
                             f"data_after_retry={mesh.history_data_after_delayed_retry}, "
                             f"pending={pending}, "
                             f"page_ends={mesh.stats['C']['addressed_page_ends_received']}")
                return True
            if command == "sync":
                station, _, origin_station = rest.partition(" ")
                station, origin_station = station.upper(), origin_station.strip().upper()
                if station not in chats:
                    mesh.log("usage: sync A|C [origin A|C]")
                else:
                    if origin_station in chats and station in {"A", "C"}:
                        # Ask this edge station to fetch an origin through its only
                        # neighbor, B. This isolates the store-and-forward path
                        # from unrelated, simultaneous inventory exchanges.
                        neighbor = chats["B"].store
                        origin = station_node_ids[origin_station]
                        mesh.history_drop_match = (chats[station].store.node, origin)
                        if (mesh.delay_first_history_page_end > 0 and station == "C" and
                                origin_station == "A" and
                                not mesh.delayed_history_page_end):
                            mesh.delay_history_page_end_armed = True
                        if ((mesh.drop_first_history_fragment or
                             mesh.drop_first_history_page_end) and station == "C" and
                                origin_station == "A" and
                                not mesh.dropped_history_fragment and
                                not mesh.dropped_history_page_end):
                            mesh.history_drop_armed = True
                        if (mesh.drop_first_proof_fragment and station == "C" and
                                origin_station == "A"):
                            origin, seq = sent_messages.get("A", ("", 0))
                            relay_proof = chats["B"].store.proof_for(origin, seq)
                            already_at_receiver = chats["C"].store.proof_for(origin, seq)
                            if (not mesh.expected_proof or
                                    relay_proof != mesh.expected_proof or
                                    already_at_receiver is not None):
                                verification_failed = True
                                mesh.log("PROOF TEST SETUP FAIL: expected complete proof at B "
                                         "and no proof at C before repair trial")
                            else:
                                mesh.proof_drop_armed = True
                                mesh.proof_test_active = True
                        high = station_highwater.get(origin_station, 0)
                        if origin_station not in stopped_stations:
                            high = chats[origin_station].store.highwater(origin)
                        elif origin_station == "A":
                            offline_origin_syncs.append(origin)
                        with chats[station].pending_lock:
                            chats[station].pending.clear()
                            chats[station].pending_sync_requests.clear()
                            chats[station].pending_proofs.clear()
                            chats[station].pending_sync_windows.clear()
                            chats[station].completed_sync_windows.clear()
                        chats[station]._handle_inventory_entry(
                            neighbor.node, origin, 1, high)
                    elif origin_station:
                        mesh.log("usage: sync A|C [origin A|C]")
                    else:
                        chats[station].sync(retry=True)
                return True
            if command == "forget":
                station, _, origin_station = rest.partition(" ")
                station, origin_station = station.upper(), origin_station.upper()
                if station not in chats or origin_station not in chats:
                    mesh.log("usage: forget C A (remove A's cached messages from C)")
                else:
                    store = chats[station].store
                    origin = chats[origin_station].store.node
                    with store.lock:
                        store.db.execute("DELETE FROM messages WHERE origin=?", (origin,))
                        store.db.execute("DELETE FROM unread_messages WHERE origin=?", (origin,))
                        contiguous = store._scan_contiguous_locked(origin)
                        store.db.execute(
                            "INSERT INTO origin_highwater(origin,contiguous) VALUES(?,?) "
                            "ON CONFLICT(origin) DO UPDATE SET contiguous=excluded.contiguous",
                            (origin, contiguous))
                        store.db.commit()
                    # Forget also clears volatile sync completion state so the
                    # test station will ask for this history again.
                    with chats[station].pending_lock:
                        chats[station].completed_sync_windows.clear()
                        chats[station].pending.clear()
                        chats[station].pending_sync_requests.clear()
                        chats[station].sync_repair_attempts.clear()
                    mesh.log(f"[{station}] removed cached history from {origin_station}")
                return True
            if command in {"stop-station", "start-station"}:
                station = rest.strip().upper()
                if station not in chats:
                    mesh.log(f"usage: {command} A|B|C")
                    verification_failed = True
                    return True
                if command == "stop-station":
                    if station in stopped_stations:
                        mesh.log(f"[{station}] is already stopped")
                        verification_failed = True
                        return True
                    chat, radio = chats[station], radios[station]
                    chat.sync_stop.set()
                    if chat.sync_thread and chat.sync_thread is not threading.current_thread():
                        chat.sync_thread.join(timeout=2)
                    mesh.detach(station, radio)
                    chat.close()
                    stopped_stations.add(station)
                    stopped_once.add(station)
                    mesh.log(f"[{station}] station process stopped; radio receiver detached")
                    return True
                if station not in stopped_stations:
                    mesh.log(f"[{station}] must be stopped before it can be restarted")
                    verification_failed = True
                    return True
                chat = Chat(make_args(station, station_nicks[station],
                                      station_db_paths[station], opts.transport,
                                      opts.sync_retry_seconds))
                radio = SimRadio(station, chat.store.node, mesh, opts.batch_window)
                radio.on_frame = chat._on_frame
                chat.data = radio
                chats[station] = chat
                radios[station] = radio
                chat.start()
                stopped_stations.remove(station)
                station_restarts[station] += 1
                mesh.log(f"[{station}] station process restarted from its durable database")
                return True
            if command in {"partition", "rejoin", "verify-partition"}:
                left, _, right = rest.partition(" ")
                left, right = left.upper(), right.strip().upper()
                if (left not in chats or right not in chats or left == right):
                    mesh.log(f"usage: {command} A B (two distinct stations)")
                    if command == "verify-partition":
                        verification_failed = True
                    return True
                link = tuple(sorted((left, right)))
                if command == "partition":
                    with mesh.lock:
                        partition_baselines[link] = {
                            left: chats[left].store.highwater(chats[left].store.node),
                            right: chats[right].store.highwater(chats[right].store.node)}
                        partitioned_links.add(link)
                        mesh.neighbors[left].discard(right)
                        mesh.neighbors[right].discard(left)
                    mesh.log(f"PARTITION: {left} and {right} can no longer hear each other")
                elif command == "rejoin":
                    with mesh.lock:
                        mesh.neighbors[left].add(right)
                        mesh.neighbors[right].add(left)
                        partitioned_links.discard(link)
                    mesh.log(f"REJOIN: {left} and {right} can hear each other again")
                else:
                    baseline = partition_baselines.get(link)
                    isolated = link in partitioned_links
                    if baseline:
                        left_seen = chats[right].store.messages_between(
                            chats[left].store.node, baseline[left], 0xFFFFFFFF, limit=100)
                        right_seen = chats[left].store.messages_between(
                            chats[right].store.node, baseline[right], 0xFFFFFFFF, limit=100)
                    else:
                        left_seen = right_seen = [object()]
                    if isolated and not left_seen and not right_seen:
                        mesh.log(f"PARTITION PASS: no post-partition records crossed {left}↔{right}")
                    else:
                        verification_failed = True
                        mesh.log(f"PARTITION FAIL: isolated={isolated}, crossed records="
                                 f"{len(left_seen) + len(right_seen)}")
                return True
            if command == "wait-radio-idle":
                deadline = time.monotonic() + 20
                quiet_since = None
                while time.monotonic() < deadline:
                    queues_empty = True
                    for radio in radios.values():
                        with radio.condition:
                            queues_empty = (queues_empty and not any(radio.pending.values())
                                            and radio.processing == 0)
                    with mesh.lock:
                        mesh_idle = not mesh.active and not mesh.delivery_timers
                    if queues_empty and mesh_idle:
                        quiet_since = quiet_since or time.monotonic()
                        if time.monotonic() - quiet_since >= 0.25:
                            break
                    else:
                        quiet_since = None
                    time.sleep(0.05)
                else:
                    verification_failed = True
                    mesh.log("RADIO IDLE FAIL: simulator TX/delivery queues did not drain")
                    return True
                mesh.log("RADIO IDLE PASS: simulator TX/delivery queues drained")
                return True
            if command == "history":
                station = rest.strip().upper()
                if station not in chats:
                    mesh.log("usage: history A")
                else:
                    for message in history_for_station(station, 100):
                        mesh.log(f"[{station}] {message['n']}: {message['b']}")
                return True
            if command == "verify":
                histories = {
                    name: {(row["o"], row["s"], row["c"], row["n"], row["b"])
                           for row in history_for_station(name, 10000)}
                    for name in chats
                }
                expected = set().union(*histories.values())
                verification_failed = verification_failed or not expected or any(
                    messages != expected for messages in histories.values())
                if mesh.drop_first_history_fragment:
                    repairs = mesh.stats["C"]["repairs"]
                    relay_rx = mesh.stats["B"]["message_fragments_received"]
                    relay_tx = mesh.stats["B"]["foreign_message_transmissions"]
                    if (not mesh.dropped_history_fragment or repairs != 1 or
                            relay_tx != relay_rx + 1):
                        verification_failed = True
                        mesh.log("VERIFY FAIL: forced fragment loss did not produce exactly one "
                                 "fragment repair through B "
                                 f"(C repair requests={repairs}, B fragments rx/tx="
                                 f"{relay_rx}/{relay_tx})")
                    else:
                        mesh.log("REPAIR PASS: C requested one missing fragment and B "
                                 "sent only that repair after the initial page")
                if mesh.drop_first_history_page_end:
                    retried = any(count > 1 for key, count in
                                  mesh.sync_query_counts.items()
                                  if key[0] == "C" and key[1] == chats["B"].store.node and
                                  key[2] == chats["A"].store.node and not key[5])
                    if (not mesh.dropped_history_page_end or not retried or
                            not mesh.history_manifest_retry_seen or
                            mesh.history_data_after_manifest_retry != 0):
                        verification_failed = True
                        mesh.log("VERIFY FAIL: lost page-end did not recover with a manifest-only retry "
                                 f"(retried={retried}, manifest_retry_seen="
                                 f"{mesh.history_manifest_retry_seen}, B data after retry="
                                 f"{mesh.history_data_after_manifest_retry})")
                    else:
                        mesh.log("PAGE-END PASS: retry recovered the page without resending its fragments")
                if verification_failed:
                    for name, messages in histories.items():
                        missing = expected - messages
                        mesh.log(f"VERIFY FAIL {name}: {len(messages)}/{len(expected)}; "
                                 f"missing {len(missing)}")
                else:
                    mesh.log(f"VERIFY PASS: all stations have {len(expected)} messages")
                return True
            if command == "verify-quiet":
                transmitted = sum(stat["frames"] for stat in mesh.stats.values())
                queued = sum(len(radio.pending[0]) + len(radio.pending[1])
                             for radio in radios.values())
                if transmitted or queued:
                    verification_failed = True
                    mesh.log(f"QUIET FAIL: startup queued {queued} records and sent "
                             f"{transmitted} frames")
                else:
                    mesh.log("QUIET PASS: receive-only startup sent no KISS frames")
                return True
            if command == "verify-bundling":
                bundled = sum(stat["message_bundles"] for stat in mesh.stats.values())
                if bundled:
                    mesh.log(f"BUNDLE PASS: {bundled} radio frames carried multiple chat messages")
                else:
                    verification_failed = True
                    mesh.log("BUNDLE FAIL: no radio frame carried multiple chat messages")
                return True
            if command == "verify-no-echo":
                echoes = sum(stat["addressed_echoes"] for stat in mesh.stats.values())
                received = (
                    mesh.stats["C"]["history_data_received"] > 0 and
                    mesh.stats["C"]["addressed_page_ends_received"] > 0 and
                    mesh.stats["A"]["message_fragments_received"] > 0)
                listener_retransmissions = mesh.stats["C"]["foreign_message_transmissions"]
                if echoes or not received or listener_retransmissions:
                    verification_failed = True
                    mesh.log("ECHO FAIL: "
                             f"addressed exchanges observed={received}, "
                             f"addressed echoes={echoes}, "
                             f"C foreign fragment transmissions={listener_retransmissions}")
                else:
                    mesh.log("GOSSIP PASS: C heard and cached the broadcast page "
                             "without retransmitting its fragments")
                return True
            if command == "stats":
                mesh.report()
                return True
            if command == "wait":
                try:
                    time.sleep(max(0, min(float(rest), 3600)))
                except ValueError:
                    mesh.log("usage: wait SECONDS")
                return True
            mesh.log(f"unknown command: {command}")
            return True

        if opts.script:
            with opts.script.open(encoding="utf-8") as stream:
                for line in stream:
                    if not execute(line):
                        break
            time.sleep(min(3.0, 3.0 * opts.time_scale))
            mesh.report()
        else:
            while True:
                try:
                    if not execute(input("sim> ")):
                        break
                except (EOFError, KeyboardInterrupt):
                    break
        if opts.json_report:
            if not checks_run:
                verification_failed = True
                mesh.log("REPORT FAIL: scripted run executed no verification command")
            with mesh.lock:
                station_metrics = {name: dict(values) for name, values in mesh.stats.items()}
                link_map = {name: sorted(list(peers))
                            for name, peers in mesh.neighbors.items()}
            station_messages = {}
            for name in STATIONS:
                rows = history_for_station(name, 10000)
                station_messages[name] = [
                    {"origin": row["o"], "sequence": int(row["s"]),
                     "channel": row["c"], "nick": row["n"], "timestamp": int(row["ts"]),
                     "body": row["b"]}
                    for row in sorted(rows, key=lambda item: (item["o"], int(item["s"])))
                ]
            proof_identity = sent_messages.get("A")
            proof_restored = False
            if proof_identity and mesh.expected_proof is not None:
                proof_restored = (
                    chats["C"].store.proof_for(*proof_identity) == mesh.expected_proof)
            proof_status_c = (chats["C"].store.auth_for(*proof_identity)
                              if proof_identity else {"status": "unavailable", "signer": ""})
            opts.json_report.parent.mkdir(parents=True, exist_ok=True)
            opts.json_report.write_text(json.dumps({
                "schema": 1,
                "passed": not verification_failed,
                "checks": checks_run,
                "config": {"seed": opts.seed, "topology": opts.topology,
                           "transport": opts.transport, "loss": opts.loss,
                           "loss_model": opts.loss_model,
                           "burst_recovery": opts.burst_recovery,
                           "drop_first_image_fragment": opts.drop_first_image_fragment,
                           "drop_first_proof_fragment": opts.drop_first_proof_fragment,
                           "delay_first_history_page_end":
                               opts.delay_first_history_page_end,
                           "bitrate": opts.bitrate, "propagation": opts.propagation,
                           "interframe": opts.interframe, "time_scale": opts.time_scale,
                           "sync_retry_seconds": opts.sync_retry_seconds},
                "faults": {
                    "dropped_proof_fragment": mesh.dropped_proof_fragment,
                    "dropped_proof_index": mesh.dropped_proof_index,
                    "proof_page_requests": mesh.proof_page_requests,
                    "proof_page_rx_baseline": mesh.proof_page_rx_baseline,
                    "proof_page_rx_total": mesh.stats["C"]["proof_fragments_received"],
                    "proof_restored_byte_identically": proof_restored,
                    "proof_expected_signed": mesh.proof_expected_signed,
                    "proof_verified_at_C": proof_status_c["status"] == "verified",
                    "proof_signer_at_C": proof_status_c["signer"],
                    "proof_body_replayed": (
                        mesh.proof_history_rx_baseline is not None and
                        mesh.stats["C"]["history_data_received"] !=
                        mesh.proof_history_rx_baseline),
                    "dropped_history_page_end": mesh.dropped_history_page_end,
                    "history_manifest_retry_seen": mesh.history_manifest_retry_seen,
                    "history_data_after_manifest_retry":
                        mesh.history_data_after_manifest_retry,
                    "delayed_history_page_end": mesh.delayed_history_page_end,
                    "delayed_history_page_end_count":
                        mesh.delayed_history_page_end_count,
                    "delayed_history_retry_seen": mesh.delayed_history_retry_seen,
                    "history_data_after_delayed_retry":
                        mesh.history_data_after_delayed_retry,
                },
                "links": link_map,
                "elapsed_seconds": time.monotonic() - mesh.started,
                "stations": station_metrics,
                "messages": station_messages,
                "signed_verifications": signed_verifications,
                "station_restarts": station_restarts,
                "stations_stopped_during_run": sorted(stopped_once),
                "offline_origin_syncs": offline_origin_syncs,
            }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    finally:
        for chat in chats.values():
            chat.sync_stop.set()
        for chat in chats.values():
            if chat.sync_thread and chat.sync_thread.is_alive():
                chat.sync_thread.join(timeout=2)
        mesh.close()
        for radio in radios.values():
            radio.close()
        for name, chat in chats.items():
            if name not in stopped_stations:
                chat.close()
        if "stop" in locals():
            stop.set()
        temp.cleanup()
    if verification_failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
