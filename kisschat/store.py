"""Small durable message store and fragment assembler."""
from __future__ import annotations

import sqlite3
import threading
import time
import uuid
import hashlib
import re

from .protocol import (MAX_CACHED_THUMBNAIL_BYTES, MAX_PARTS, MIN_CHUNK_BYTES,
                       _message_chunk_limit)
from .images import MAX_EDGE, is_valid_thumbnail
from .mentions import valid_mention_name


CHANNEL_NAME = re.compile(r"^#[A-Za-z0-9_-]{1,31}$")
PARTIAL_LIMITS = {"fragments": 256, "proof_fragments": 64, "image_fragments": 32}
MAX_IMAGE_CACHE_BYTES = 64 * 1024 * 1024


class Store:
    def __init__(self, path: str, callsign: str):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.create_function("kisschat_casefold", 1,
                                 lambda value: value.casefold(), deterministic=True)
        self.db.execute("PRAGMA journal_mode=WAL")
        # Relay history and receipts must survive a node losing power just
        # after acknowledging a committed record.
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS origin_highwater (
              origin TEXT PRIMARY KEY, contiguous INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS unread_messages (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, channel TEXT NOT NULL,
              mention INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(origin,seq));
            CREATE INDEX IF NOT EXISTS unread_messages_channel ON unread_messages(channel);
            CREATE TABLE IF NOT EXISTS messages (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, channel TEXT NOT NULL,
              nick TEXT NOT NULL, ts INTEGER NOT NULL, body TEXT NOT NULL,
              PRIMARY KEY(origin,seq));
            CREATE INDEX IF NOT EXISTS messages_channel_time ON messages(channel,ts);
            CREATE INDEX IF NOT EXISTS messages_recent_inventory ON messages(ts,origin,seq);
            CREATE INDEX IF NOT EXISTS messages_origin_recent ON messages(origin,ts,seq);
            CREATE TABLE IF NOT EXISTS fragments (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, idx INTEGER NOT NULL,
              count INTEGER NOT NULL, channel TEXT NOT NULL, nick TEXT NOT NULL,
              ts INTEGER NOT NULL, body BLOB NOT NULL,
              chunk_size INTEGER NOT NULL DEFAULT 120,
              PRIMARY KEY(origin,seq,idx));
            CREATE TABLE IF NOT EXISTS mention_receipts (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, recipient TEXT NOT NULL,
              mention TEXT NOT NULL, received_at INTEGER NOT NULL,
              PRIMARY KEY(origin,seq,recipient,mention));
            CREATE TABLE IF NOT EXISTS receipt_relay (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, recipient_id TEXT NOT NULL,
              nick TEXT NOT NULL, mention TEXT NOT NULL, received_at INTEGER NOT NULL,
              delivered INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
              next_attempt INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(origin,seq,recipient_id,mention));
            CREATE TABLE IF NOT EXISTS message_auth (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, proof BLOB,
              status TEXT NOT NULL DEFAULT 'unsigned', signer TEXT NOT NULL DEFAULT '',
              detail TEXT NOT NULL DEFAULT '',
              PRIMARY KEY(origin,seq));
            CREATE TABLE IF NOT EXISTS proof_fragments (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, idx INTEGER NOT NULL,
              count INTEGER NOT NULL, proof BLOB NOT NULL,
              PRIMARY KEY(origin,seq,idx));
            CREATE TABLE IF NOT EXISTS attachments (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, digest TEXT NOT NULL,
              size INTEGER NOT NULL, width INTEGER NOT NULL, height INTEGER NOT NULL,
              PRIMARY KEY(origin,seq));
            CREATE TABLE IF NOT EXISTS attachment_blobs (
              digest TEXT PRIMARY KEY, body BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS image_fragments (
              origin TEXT NOT NULL, seq INTEGER NOT NULL, digest TEXT NOT NULL,
              idx INTEGER NOT NULL, count INTEGER NOT NULL, body BLOB NOT NULL,
              PRIMARY KEY(origin,seq,idx));
        """)
        self._migrate_schema()
        self.db.commit()
        with self.lock:
            self.node = self._meta("node") or uuid.uuid4().hex
            self._set_meta("node", self.node)
            self._set_meta("callsign", callsign.upper())
            self.callsign = callsign.upper()
            self.seq = int(self._meta("seq") or "0")
            self.db.commit()

    def _meta(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                         "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

    def _bound_partial_transfers(self, table: str, origin: str, seq: int) -> None:
        """Keep interrupted fragment assemblies from growing an always-on node forever."""
        limit = PARTIAL_LIMITS[table]
        count = int(self.db.execute(
            f"SELECT COUNT(*) FROM (SELECT 1 FROM {table} GROUP BY origin,seq)"
        ).fetchone()[0])
        while count > limit:
            least_recent = self.db.execute(
                f"SELECT origin,seq FROM {table} GROUP BY origin,seq "
                "ORDER BY MAX(rowid) LIMIT 2").fetchall()
            if not least_recent:
                break
            victim = next((row for row in least_recent
                           if (row["origin"], int(row["seq"])) != (origin, seq)),
                          least_recent[0])
            self.db.execute(f"DELETE FROM {table} WHERE origin=? AND seq=?",
                             (victim["origin"], victim["seq"]))
            count -= 1

    def _migrate_schema(self) -> None:
        """Migrate durable receipts and unread state without losing old rows."""
        schemas = {
            "mention_receipts": (
                "CREATE TABLE mention_receipts_new (origin TEXT NOT NULL, seq INTEGER NOT NULL, "
                "recipient TEXT NOT NULL, mention TEXT NOT NULL, received_at INTEGER NOT NULL, "
                "PRIMARY KEY(origin,seq,recipient,mention))",
                "INSERT OR IGNORE INTO mention_receipts_new SELECT * FROM mention_receipts",
            ),
            "receipt_relay": (
                "CREATE TABLE receipt_relay_new (origin TEXT NOT NULL, seq INTEGER NOT NULL, "
                "recipient_id TEXT NOT NULL, nick TEXT NOT NULL, mention TEXT NOT NULL, "
                "received_at INTEGER NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, "
                "attempts INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL DEFAULT 0, "
                "PRIMARY KEY(origin,seq,recipient_id,mention))",
                "INSERT OR IGNORE INTO receipt_relay_new SELECT * FROM receipt_relay",
            ),
        }
        self.db.execute("BEGIN IMMEDIATE")
        try:
            unread_columns = {row["name"] for row in
                              self.db.execute("PRAGMA table_info(unread_messages)")}
            if "mention" not in unread_columns:
                self.db.execute("ALTER TABLE unread_messages ADD COLUMN mention "
                                "INTEGER NOT NULL DEFAULT 0")
            fragment_columns = {row["name"] for row in
                                self.db.execute("PRAGMA table_info(fragments)")}
            if "chunk_size" not in fragment_columns:
                self.db.execute("ALTER TABLE fragments ADD COLUMN chunk_size "
                                "INTEGER NOT NULL DEFAULT 120")
            for table, (create_sql, copy_sql) in schemas.items():
                info = self.db.execute(f"PRAGMA table_info({table})").fetchall()
                primary_key = [row["name"] for row in sorted(info, key=lambda row: row["pk"])
                               if row["pk"]]
                expected = (["origin", "seq", "recipient", "mention"]
                            if table == "mention_receipts" else
                            ["origin", "seq", "recipient_id", "mention"])
                if primary_key == expected:
                    continue
                self.db.execute(f"DROP TABLE IF EXISTS {table}_new")
                self.db.execute(create_sql)
                self.db.execute(copy_sql)
                self.db.execute(f"DROP TABLE {table}")
                self.db.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
        except Exception:
            self.db.rollback()
            raise

    def saved_nick(self) -> str | None:
        with self.lock:
            return self._meta("nick")

    def saved_channel(self) -> str | None:
        with self.lock:
            return self._meta("channel")

    def save_channel(self, channel: str) -> None:
        if not CHANNEL_NAME.fullmatch(channel):
            raise ValueError("invalid channel name")
        with self.lock:
            self.db.execute("INSERT OR IGNORE INTO channels(name) VALUES(?)", (channel,))
            self._set_meta("channel", channel)
            self.db.commit()

    def save_nick(self, nick: str) -> None:
        with self.lock:
            self._set_meta("nick", nick)
            self.db.commit()

    def add_channel(self, channel: str) -> bool:
        if not CHANNEL_NAME.fullmatch(channel):
            return False
        with self.lock:
            cursor = self.db.execute("INSERT OR IGNORE INTO channels(name) VALUES(?)",
                                     (channel,))
            self.db.commit()
            return cursor.rowcount == 1

    def insert(self, origin: str, seq: int, channel: str, nick: str,
               timestamp: int, body: str) -> bool:
        with self.lock:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO messages VALUES(?,?,?,?,?,?)",
                (origin, seq, channel, nick, timestamp, body))
            if cursor.rowcount == 1:
                self._advance_contiguous_locked(origin)
                self._increment_message_count_locked()
            self.db.commit()
            return cursor.rowcount == 1

    def insert_local(self, channel: str, nick: str, timestamp: int, body: str,
                     image: tuple[bytes, int, int] | None = None,
                     signer: str = "") -> dict:
        """Commit a local message and all its durable image data atomically."""
        if (not CHANNEL_NAME.fullmatch(channel) or not nick or
                len(nick.encode("utf-8")) > 16 or
                not all(char.isprintable() for char in nick) or
                type(timestamp) is not int or not 0 <= timestamp <= 0xFFFFFFFF or
                not isinstance(body, str) or len(body.encode("utf-8")) > 4096 or
                not all(char.isprintable() or char in "\t\n" for char in body)):
            raise ValueError("invalid local message")
        attachment = None
        if image is not None:
            if not isinstance(image, tuple) or len(image) != 3:
                raise ValueError("invalid local thumbnail")
            image_body, width, height = image
            if (not isinstance(image_body, bytes) or
                    not 0 < len(image_body) <= MAX_CACHED_THUMBNAIL_BYTES or
                    type(width) is not int or not 1 <= width <= MAX_EDGE or
                    type(height) is not int or not 1 <= height <= MAX_EDGE or
                    not is_valid_thumbnail(image_body)):
                raise ValueError("invalid local thumbnail")
            digest = hashlib.sha256(image_body).hexdigest()
            attachment = {"digest": digest, "size": len(image_body),
                          "width": width, "height": height}
        if not body and attachment is None:
            raise ValueError("a local message needs text or an image")

        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                seq = int(self._meta("seq") or "0") + 1
                if seq > 0xFFFFFFFF:
                    raise OverflowError("local message sequence reached its wire limit")
                self._set_meta("seq", str(seq))
                self.db.execute("INSERT INTO messages VALUES(?,?,?,?,?,?)",
                                (self.node, seq, channel, nick, timestamp, body))
                self._advance_contiguous_locked(self.node)
                self._increment_message_count_locked()
                message = {"o": self.node, "s": seq, "c": channel,
                           "n": nick, "ts": timestamp, "b": body}
                if attachment is not None:
                    self.db.execute("INSERT INTO attachments VALUES(?,?,?,?,?,?)",
                                    (self.node, seq, attachment["digest"],
                                     attachment["size"], attachment["width"],
                                     attachment["height"]))
                    self._cache_image_locked(attachment["digest"], image[0])
                    message["image"] = attachment
                if signer:
                    self.db.execute(
                        "INSERT INTO message_auth(origin,seq,proof,status,signer,detail) "
                        "VALUES(?,?,NULL,'pending',?,'')", (self.node, seq, signer))
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            self.seq = seq
            return message

    def add_fragment(self, part: dict) -> tuple[dict, bool] | None:
        """Persist a fragment; return a complete message and whether it is new."""
        origin, seq, index, count = part["o"], int(part["s"]), int(part["i"]), int(part["z"])
        chunk = part["b"]
        channel, nick = part.get("c", ""), part.get("n", "")
        if not isinstance(channel, str) or not isinstance(nick, str):
            return None
        adaptive = bool(part.get("adaptive", False))
        relay = bool(part.get("to", ""))
        chunk_size = int(part.get("chunk", MIN_CHUNK_BYTES))
        expected_chunk_size = (_message_chunk_limit(len(channel.encode("utf-8")),
                                                     len(nick.encode("utf-8")),
                                                     relay=relay)
                               if adaptive else MIN_CHUNK_BYTES)
        if (len(origin) != 32 or not 1 <= seq <= 0xFFFFFFFF or
                not 1 <= count <= MAX_PARTS or not 0 <= index < count or
                not isinstance(chunk, bytes) or chunk_size != expected_chunk_size or
                len(chunk) > chunk_size or
                (index < count - 1 and len(chunk) != chunk_size) or
                not CHANNEL_NAME.fullmatch(channel) or not nick or
                len(nick.encode("utf-8")) > 16 or
                not all(char.isprintable() for char in nick)):
            return None
        with self.lock:
            if self.db.execute("SELECT 1 FROM messages WHERE origin=? AND seq=?",
                               (origin, seq)).fetchone():
                return None
            common = (origin, seq, count, part["c"], part["n"], int(part["ts"]),
                      chunk_size)
            existing = self.db.execute(
                "SELECT count,channel,nick,ts,chunk_size FROM fragments "
                "WHERE origin=? AND seq=? LIMIT 1",
                (origin, seq)).fetchone()
            new_transfer = existing is None
            if existing and tuple(existing) != common[2:]:
                if tuple(existing[:4]) != common[2:6]:
                    self.db.execute("DELETE FROM fragments WHERE origin=? AND seq=?",
                                    (origin, seq))
                    self.db.commit()
                    return None
                # A partial transfer made with a different chunking scheme
                # cannot safely be mixed with this fragment. Restart it here.
                self.db.execute("DELETE FROM fragments WHERE origin=? AND seq=?",
                                (origin, seq))
                new_transfer = True
            self.db.execute(
                "INSERT OR IGNORE INTO fragments "
                "(origin,seq,idx,count,channel,nick,ts,body,chunk_size) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (origin, seq, index, count, channel, nick, int(part["ts"]),
                 chunk, chunk_size))
            if new_transfer:
                self._bound_partial_transfers("fragments", origin, seq)
            rows = self.db.execute(
                "SELECT idx,body FROM fragments WHERE origin=? AND seq=? ORDER BY idx",
                (origin, seq)).fetchall()
            if len(rows) != count:
                self.db.commit()
                return None
            try:
                raw_body = b"".join(row["body"] for row in rows)
                body = raw_body.decode("utf-8")
            except UnicodeDecodeError:
                self.db.execute("DELETE FROM fragments WHERE origin=? AND seq=?", (origin, seq))
                self.db.commit()
                return None
            if (len(raw_body) > 4096 or
                    not all(char.isprintable() or char in "\t\n" for char in body)):
                self.db.execute("DELETE FROM fragments WHERE origin=? AND seq=?", (origin, seq))
                self.db.commit()
                return None
            message = {"o": origin, "s": seq, "c": part["c"], "n": part["n"],
                       "ts": int(part["ts"]), "b": body}
            self.db.execute("DELETE FROM fragments WHERE origin=? AND seq=?", (origin, seq))
            cursor = self.db.execute("INSERT OR IGNORE INTO messages VALUES(?,?,?,?,?,?)",
                                     (origin, seq, message["c"], message["n"],
                                      message["ts"], body))
            if cursor.rowcount == 1:
                self._advance_contiguous_locked(origin)
                self._increment_message_count_locked()
            self.db.commit()
            return message, cursor.rowcount == 1

    def highwater(self, origin: str, since: int | None = None) -> int:
        with self.lock:
            if since is None:
                row = self.db.execute(
                    "SELECT MAX(seq) FROM messages WHERE origin=?", (origin,)).fetchone()
            else:
                row = self.db.execute(
                    "SELECT MAX(seq) FROM messages WHERE origin=? AND ts>=?",
                    (origin, since)).fetchone()
            return int(row[0] or 0)

    def has_message(self, origin: str, seq: int) -> bool:
        """Return whether a complete message is present, not just fragments."""
        with self.lock:
            return self.db.execute(
                "SELECT 1 FROM messages WHERE origin=? AND seq=?", (origin, seq)
            ).fetchone() is not None

    def missing_fragment_mask(self, origin: str, seq: int) -> int:
        """Return a bitmask of absent message fragments, or zero if none are cached."""
        with self.lock:
            rows = self.db.execute(
                "SELECT idx,count FROM fragments WHERE origin=? AND seq=?",
                (origin, seq)).fetchall()
        if not rows:
            return 0
        counts = {int(row["count"]) for row in rows}
        if len(counts) != 1:
            return 0
        count = counts.pop()
        received = {int(row["idx"]) for row in rows}
        return sum(1 << index for index in range(count) if index not in received)

    def _scan_contiguous_locked(self, origin: str) -> int:
        """Build the initial high-water value for a history already on disk."""
        expected = 1
        for row in self.db.execute(
                "SELECT seq FROM messages WHERE origin=? ORDER BY seq", (origin,)):
            value = int(row[0])
            if value == expected:
                expected += 1
            elif value > expected:
                break
        return expected - 1

    def _advance_contiguous_locked(self, origin: str) -> int:
        """Advance the durable contiguous sequence after inserting a message."""
        row = self.db.execute(
            "SELECT contiguous FROM origin_highwater WHERE origin=?", (origin,)
        ).fetchone()
        high = int(row[0]) if row else self._scan_contiguous_locked(origin)
        next_seq = high + 1
        if self.db.execute(
                "SELECT 1 FROM messages WHERE origin=? AND seq=?", (origin, next_seq)
        ).fetchone():
            gap = self.db.execute(
                "SELECT m.seq FROM messages AS m "
                "WHERE m.origin=? AND m.seq>? "
                "AND NOT EXISTS (SELECT 1 FROM messages AS p "
                "WHERE p.origin=m.origin AND p.seq=m.seq-1) "
                "ORDER BY m.seq LIMIT 1", (origin, next_seq)).fetchone()
            if gap:
                # The found row is immediately after a missing sequence.
                high = int(gap[0]) - 2
            else:
                high = int(self.db.execute(
                    "SELECT MAX(seq) FROM messages WHERE origin=?", (origin,)
                ).fetchone()[0] or 0)
        self.db.execute(
            "INSERT INTO origin_highwater(origin,contiguous) VALUES(?,?) "
            "ON CONFLICT(origin) DO UPDATE SET contiguous=excluded.contiguous",
            (origin, high))
        return high

    def contiguous(self, origin: str) -> int:
        """Highest sequence with every earlier message present, filling gaps."""
        with self.lock:
            row = self.db.execute(
                "SELECT contiguous FROM origin_highwater WHERE origin=?", (origin,)
            ).fetchone()
            if row:
                return int(row[0])
            high = self._scan_contiguous_locked(origin)
            self.db.execute("INSERT INTO origin_highwater(origin,contiguous) VALUES(?,?)",
                            (origin, high))
            self.db.commit()
            return high

    def inventory(self, since: int) -> list[tuple[str, int, int]]:
        with self.lock:
            # Announce only the configured recent window. Low and high include
            # sparse sequences so a cache can reveal messages beyond a gap.
            # The upper bound prevents a future-dated record from extending
            # this rolling window indefinitely.
            rows = self.db.execute(
                "SELECT origin, MIN(seq), MAX(seq) FROM messages "
                "WHERE ts>=? AND ts<=? GROUP BY origin ORDER BY origin",
                (since, int(time.time())))
            return [(str(origin), int(low), int(high))
                    for origin, low, high in rows]

    def recent_bounds(self, origin: str, since: int) -> tuple[int, int] | None:
        with self.lock:
            row = self.db.execute(
                "SELECT MIN(seq), MAX(seq) FROM messages "
                "WHERE origin=? AND ts>=? AND ts<=?",
                (origin, since, int(time.time()))).fetchone()
            if row is None or row[0] is None:
                return None
            return int(row[0]), int(row[1])

    def messages_between(self, origin: str, after: int, through: int,
                         limit: int = 20, since: int | None = None) -> list[dict]:
        with self.lock:
            if since is None:
                rows = self.db.execute(
                    "SELECT origin AS o,seq AS s,channel AS c,nick AS n,ts,body AS b "
                    "FROM messages WHERE origin=? AND seq>? AND seq<=? "
                    "ORDER BY seq LIMIT ?",
                    (origin, after, through, max(1, min(100, int(limit)))))
            else:
                rows = self.db.execute(
                    "SELECT origin AS o,seq AS s,channel AS c,nick AS n,ts,body AS b "
                    "FROM messages WHERE origin=? AND seq>? AND seq<=? "
                    "AND ts>=? AND ts<=? "
                    "ORDER BY seq LIMIT ?",
                    (origin, after, through, since, int(time.time()),
                     max(1, min(100, int(limit)))))
            return [dict(row) for row in rows]

    def add_proof_fragment(self, part: dict) -> bytes | None:
        origin, seq, index, count = part["o"], int(part["s"]), int(part["i"]), int(part["z"])
        chunk = part["b"]
        if (len(origin) != 32 or not 1 <= seq <= 0xFFFFFFFF or
                not isinstance(chunk, bytes) or len(chunk) > 190 or
                not 1 <= count <= 255 or not 0 <= index < count):
            return None
        with self.lock:
            row = self.db.execute(
                "SELECT count FROM proof_fragments WHERE origin=? AND seq=? LIMIT 1",
                (origin, seq)).fetchone()
            new_transfer = row is None
            if row and row[0] != count:
                self.db.execute("DELETE FROM proof_fragments WHERE origin=? AND seq=?",
                                (origin, seq))
            self.db.execute("INSERT OR IGNORE INTO proof_fragments VALUES(?,?,?,?,?)",
                            (origin, seq, index, count, chunk))
            if new_transfer:
                self._bound_partial_transfers("proof_fragments", origin, seq)
            rows = self.db.execute(
                "SELECT idx,proof FROM proof_fragments WHERE origin=? AND seq=? ORDER BY idx",
                (origin, seq)).fetchall()
            if len(rows) != count:
                self.db.commit()
                return None
            proof = b"".join(row["proof"] for row in rows)
            self.db.execute("DELETE FROM proof_fragments WHERE origin=? AND seq=?", (origin, seq))
            existing = self.db.execute(
                "SELECT proof FROM message_auth WHERE origin=? AND seq=?",
                (origin, seq)).fetchone()
            if existing and existing[0] is not None and bytes(existing[0]) == proof:
                self.db.commit()
                return None
            self.db.execute("INSERT INTO message_auth(origin,seq,proof,status) VALUES(?,?,?,'pending') "
                            "ON CONFLICT(origin,seq) DO UPDATE SET proof=excluded.proof,status='pending',"
                            "signer='',detail=''", (origin, seq, proof))
            self.db.commit()
            return proof

    def set_auth(self, origin: str, seq: int, proof: bytes | None, status: str,
                 signer: str = "", detail: str = "") -> None:
        if status not in {"pending", "signing_failed", "verified", "untrusted",
                          "unverified", "invalid"}:
            raise ValueError("invalid message authentication state")
        if status == "signing_failed" and proof is not None:
            raise ValueError("a failed signing record cannot contain a proof")
        with self.lock:
            self.db.execute("INSERT INTO message_auth(origin,seq,proof,status,signer,detail) "
                            "VALUES(?,?,?,?,?,?) ON CONFLICT(origin,seq) DO UPDATE SET "
                            "proof=excluded.proof,status=excluded.status,signer=excluded.signer,"
                            "detail=excluded.detail", (origin, seq, proof, status, signer, detail))
            self.db.commit()

    def refresh_auth_policy(self, revision: str) -> int:
        """Invalidate cached proof decisions after local verification policy changes."""
        if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{64}", revision):
            raise ValueError("invalid ARAP verification revision")
        with self.lock:
            if self._meta("auth_verification_revision") == revision:
                return 0
            self.db.execute("BEGIN IMMEDIATE")
            try:
                cursor = self.db.execute(
                    "UPDATE message_auth SET status='pending',"
                    "detail='ARAP verification policy changed; rechecking' "
                    "WHERE proof IS NOT NULL AND status IN "
                    "('verified','untrusted','unverified','invalid')")
                self._set_meta("auth_verification_revision", revision)
                changed = cursor.rowcount
                self.db.commit()
                return changed
            except Exception:
                self.db.rollback()
                raise

    def auth_for(self, origin: str, seq: int) -> dict:
        with self.lock:
            row = self.db.execute("SELECT status,signer,detail,proof FROM message_auth "
                                  "WHERE origin=? AND seq=?", (origin, seq)).fetchone()
            if row:
                return dict(row)
            return {"status": "unsigned", "signer": "", "detail": "", "proof": None}

    def proof_for(self, origin: str, seq: int) -> bytes | None:
        with self.lock:
            row = self.db.execute("SELECT proof FROM message_auth WHERE origin=? AND seq=?",
                                  (origin, seq)).fetchone()
            return bytes(row[0]) if row and row[0] is not None else None

    def pending_local_signatures(self, origin: str, limit: int = 32) -> list[dict]:
        """Return committed local messages whose requested signature is unfinished."""
        with self.lock:
            rows = [dict(row) for row in self.db.execute(
                "SELECT m.origin AS o,m.seq AS s,m.channel AS c,m.nick AS n,m.ts,m.body AS b, "
                "att.digest AS image_digest,att.size AS image_size, "
                "att.width AS image_width,att.height AS image_height "
                "FROM messages AS m JOIN message_auth AS auth USING(origin,seq) "
                "LEFT JOIN attachments AS att USING(origin,seq) "
                "WHERE m.origin=? AND auth.status IN ('pending','signing_failed') "
                "AND auth.proof IS NULL "
                "ORDER BY m.seq LIMIT ?", (origin, max(1, int(limit))))]
            for message in rows:
                if message["image_digest"]:
                    message["image"] = {
                        "digest": message.pop("image_digest"),
                        "size": message.pop("image_size"),
                        "width": message.pop("image_width"),
                        "height": message.pop("image_height"),
                    }
                else:
                    for column in ("image_digest", "image_size", "image_width", "image_height"):
                        message.pop(column)
            return rows

    def pending_auth_proofs(self, limit: int = 32,
                            after: tuple[str, int] | None = None
                            ) -> list[tuple[str, int, bytes]]:
        """Fetch stored proofs for pending or refreshed trust verification."""
        with self.lock:
            query = (
                "SELECT a.origin,a.seq,a.proof FROM message_auth AS a "
                "JOIN messages AS m USING(origin,seq) "
                "WHERE a.status IN ('pending','untrusted','unverified') "
                "AND a.proof IS NOT NULL")
            params: tuple = ()
            if after is not None:
                query += " AND (a.origin>? OR (a.origin=? AND a.seq>?))"
                params = (after[0], after[0], int(after[1]))
            query += " ORDER BY a.origin,a.seq LIMIT ?"
            rows = self.db.execute(query, (*params, max(1, int(limit)))).fetchall()
            return [(row["origin"], int(row["seq"]), bytes(row["proof"]))
                    for row in rows]

    def set_attachment(self, origin: str, seq: int, digest: str, size: int,
                       width: int, height: int) -> bool:
        if (not isinstance(origin, str) or len(origin) != 32 or
                any(char not in "0123456789abcdef" for char in origin) or
                not isinstance(digest, str) or len(digest) != 64 or
                any(char not in "0123456789abcdef" for char in digest) or
                type(seq) is not int or not 1 <= seq <= 0xFFFFFFFF or
                type(size) is not int or not 0 < size <= MAX_CACHED_THUMBNAIL_BYTES or
                type(width) is not int or not 1 <= width <= MAX_EDGE or
                type(height) is not int or not 1 <= height <= MAX_EDGE):
            return False
        with self.lock:
            existing = self.db.execute(
                "SELECT digest,size,width,height FROM attachments WHERE origin=? AND seq=?",
                (origin, seq)).fetchone()
            if existing:
                return tuple(existing) == (digest, size, width, height)
            self.db.execute("INSERT INTO attachments VALUES(?,?,?,?,?,?)",
                            (origin, seq, digest, size, width, height))
            self.db.commit()
        return True

    def attachment_for(self, origin: str, seq: int) -> dict | None:
        with self.lock:
            row = self.db.execute("SELECT origin,seq,digest,size,width,height FROM attachments "
                                  "WHERE origin=? AND seq=?", (origin, seq)).fetchone()
            return dict(row) if row else None

    def find_attachment(self, prefix: str, seq: int) -> dict | None:
        prefix = prefix.lower()
        if not 1 <= len(prefix) <= 32 or any(ch not in "0123456789abcdef" for ch in prefix):
            return None
        with self.lock:
            row = self.db.execute("SELECT origin,seq,digest,size,width,height FROM attachments "
                                  "WHERE substr(origin,1,?)=? AND seq=? ORDER BY origin LIMIT 2",
                                  (len(prefix), prefix, seq)).fetchall()
            return dict(row[0]) if len(row) == 1 else None

    def put_image(self, digest: str, body: bytes) -> bool:
        if (len(body) > MAX_CACHED_THUMBNAIL_BYTES or
                hashlib.sha256(body).hexdigest() != digest or
                not is_valid_thumbnail(body)):
            return False
        with self.lock:
            self._cache_image_locked(digest, body)
            self.db.commit()
        return True

    def _cache_image_locked(self, digest: str, body: bytes) -> None:
        self.db.execute("INSERT OR REPLACE INTO attachment_blobs VALUES(?,?)", (digest, body))
        total = int(self.db.execute(
            "SELECT COALESCE(SUM(length(body)),0) FROM attachment_blobs").fetchone()[0])
        while total > MAX_IMAGE_CACHE_BYTES:
            oldest = self.db.execute(
                "SELECT digest,length(body) AS size FROM attachment_blobs "
                "ORDER BY rowid LIMIT 1").fetchone()
            if not oldest:
                break
            self.db.execute("DELETE FROM attachment_blobs WHERE digest=?",
                            (oldest["digest"],))
            total -= int(oldest["size"])

    def image_for(self, digest: str) -> bytes | None:
        with self.lock:
            row = self.db.execute("SELECT body FROM attachment_blobs WHERE digest=?", (digest,)).fetchone()
            return bytes(row[0]) if row else None

    def add_image_fragment(self, part: dict) -> bytes | None:
        origin, seq, digest = part["o"], int(part["s"]), part["h"]
        index, count, chunk = int(part["i"]), int(part["z"]), part["b"]
        attachment = self.attachment_for(origin, seq)
        if (not attachment or attachment["digest"] != digest or count > 200 or
                not isinstance(chunk, bytes) or len(chunk) > 144):
            return None
        with self.lock:
            row = self.db.execute("SELECT digest,count FROM image_fragments WHERE origin=? AND seq=? LIMIT 1",
                                  (origin, seq)).fetchone()
            new_transfer = row is None
            if row and (row[0] != digest or int(row[1]) != count):
                self.db.execute("DELETE FROM image_fragments WHERE origin=? AND seq=?", (origin, seq))
            self.db.execute("INSERT OR IGNORE INTO image_fragments VALUES(?,?,?,?,?,?)",
                            (origin, seq, digest, index, count, chunk))
            if new_transfer:
                self._bound_partial_transfers("image_fragments", origin, seq)
            rows = self.db.execute("SELECT idx,body FROM image_fragments WHERE origin=? AND seq=? "
                                   "ORDER BY idx", (origin, seq)).fetchall()
            if len(rows) != count:
                self.db.commit()
                return None
            body = b"".join(bytes(item["body"]) for item in rows)
            self.db.execute("DELETE FROM image_fragments WHERE origin=? AND seq=?", (origin, seq))
            self.db.commit()
        # The digest protects the bytes; the advertised size also bounds the
        # reassembled transfer to the exact thumbnail announced in history.
        return body if len(body) == int(attachment["size"]) and self.put_image(digest, body) else None

    def missing_image_indices(self, origin: str, seq: int, count: int) -> list[int]:
        """Return missing thumbnail chunks, resetting progress from older layouts."""
        if not 1 <= count <= 200:
            return []
        with self.lock:
            rows = self.db.execute(
                "SELECT idx,count FROM image_fragments WHERE origin=? AND seq=?",
                (origin, seq)).fetchall()
        if rows and any(int(row["count"]) != count for row in rows):
            return list(range(count))
        received = {int(row["idx"]) for row in rows}
        return [index for index in range(count) if index not in received]

    def image_progress(self, origin: str, seq: int) -> tuple[int, int]:
        attachment = self.attachment_for(origin, seq)
        if not attachment:
            return 0, 0
        count = max(1, (int(attachment["size"]) + 138) // 139)
        missing = self.missing_image_indices(origin, seq, count)
        return count - len(missing), count

    def history(self, channel: str, limit: int = 30) -> list[dict]:
        with self.lock:
            rows = self.db.execute(
                "SELECT origin AS o,seq AS s,channel AS c,nick AS n,ts,body AS b "
                "FROM messages WHERE channel=? ORDER BY ts DESC,origin DESC,seq DESC LIMIT ?",
                (channel, limit)).fetchall()
            return [dict(row) for row in reversed(rows)]

    def history_count(self, channel: str) -> int:
        with self.lock:
            row = self.db.execute("SELECT COUNT(*) FROM messages WHERE channel=?",
                                  (channel,)).fetchone()
            return int(row[0])

    def search_history(self, channel: str, query: str, limit: int = 200) -> list[dict]:
        """Search literal message text in one channel, newest matches first then display order."""
        needle = query.strip()
        if not needle:
            return self.history(channel, limit)
        with self.lock:
            rows = self.db.execute(
                "SELECT origin AS o,seq AS s,channel AS c,nick AS n,ts,body AS b "
                "FROM messages WHERE channel=? "
                "AND instr(kisschat_casefold(body),kisschat_casefold(?))>0 "
                "ORDER BY ts DESC,origin DESC,seq DESC LIMIT ?",
                (channel, needle, max(1, int(limit)))).fetchall()
            return [dict(row) for row in reversed(rows)]

    def search_count(self, channel: str, query: str) -> int:
        needle = query.strip()
        if not needle:
            return self.history_count(channel)
        with self.lock:
            row = self.db.execute(
                "SELECT COUNT(*) FROM messages WHERE channel=? "
                "AND instr(kisschat_casefold(body),kisschat_casefold(?))>0",
                (channel, needle)).fetchone()
            return int(row[0])

    def mark_unread(self, origin: str, seq: int, channel: str, *, mention: bool = False) -> None:
        with self.lock:
            self.db.execute("INSERT OR IGNORE INTO unread_messages(origin,seq,channel,mention) "
                            "VALUES(?,?,?,?)", (origin, seq, channel, int(mention)))
            self.db.commit()

    def unread_summary(self, channel: str | None = None) -> list[dict]:
        with self.lock:
            query = ("SELECT channel AS c,COUNT(*) AS messages,SUM(mention) AS mentions "
                     "FROM unread_messages")
            if channel is not None:
                rows = self.db.execute(query + " WHERE channel=? GROUP BY channel", (channel,)).fetchall()
            else:
                rows = self.db.execute(query + " GROUP BY channel ORDER BY channel").fetchall()
            return [dict(row) for row in rows]

    def clear_unread(self, channel: str) -> None:
        with self.lock:
            self.db.execute("DELETE FROM unread_messages WHERE channel=?", (channel,))
            self.db.commit()

    def clear_local_history(self) -> int:
        """Delete this node's cached conversation and transfer history.

        Keep the node identity and sequence counter so a peer that retained an
        older copy will not see sequence identities reused after the purge.
        """
        with self.lock:
            count = int(self.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
            for table in ("messages", "unread_messages", "fragments",
                          "mention_receipts", "receipt_relay", "message_auth",
                          "proof_fragments", "attachments", "attachment_blobs",
                          "image_fragments"):
                self.db.execute(f"DELETE FROM {table}")
            self.db.execute("DELETE FROM origin_highwater")
            # The local sequence continues after purge. Treat that prefix as
            # intentionally retired so the next local message advances cleanly.
            if self.seq:
                self.db.execute("INSERT INTO origin_highwater(origin,contiguous) VALUES(?,?)",
                                (self.node, self.seq))
            self._set_meta("message_count", "0")
            self.db.commit()
            return count

    def message_count(self) -> int:
        with self.lock:
            value = self._meta("message_count")
            if value is None:
                count = int(self.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
                self._set_meta("message_count", str(count))
                self.db.commit()
                return count
            return int(value)

    def _increment_message_count_locked(self) -> None:
        """Update the durable total after a successful message insert."""
        value = self._meta("message_count")
        if value is None:
            count = int(self.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
        else:
            count = int(value) + 1
        self._set_meta("message_count", str(count))

    def messages_with_mentions(self) -> list[dict]:
        """Return stored messages that could contain an @mention for recovery."""
        with self.lock:
            return [dict(row) for row in self.db.execute(
                "SELECT origin AS o,seq AS s,nick AS n,body AS b FROM messages "
                "WHERE instr(body,'@') > 0 ORDER BY origin,seq")]

    def recent_attachments(self, channel: str, limit: int = 12) -> list[dict]:
        with self.lock:
            rows = self.db.execute(
                "SELECT a.origin,a.seq,a.digest,a.size,a.width,a.height,m.nick,m.ts "
                "FROM attachments AS a JOIN messages AS m USING(origin,seq) "
                "WHERE m.channel=? ORDER BY m.ts DESC,a.origin DESC,a.seq DESC LIMIT ?",
                (channel, limit)).fetchall()
            return [dict(row) for row in rows]

    def mark_receipt(self, origin: str, seq: int, recipient: str,
                     mention: str, received_at: int) -> bool:
        with self.lock:
            if not self.db.execute("SELECT 1 FROM messages WHERE origin=? AND seq=?",
                                   (origin, seq)).fetchone():
                return False
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO mention_receipts VALUES(?,?,?,?,?)",
                (origin, seq, recipient, mention, received_at))
            self.db.commit()
            return cursor.rowcount == 1

    def receipts_for(self, origin: str, seq: int) -> list[tuple[str, str]]:
        with self.lock:
            return [(row[0], row[1]) for row in self.db.execute(
                "SELECT recipient,mention FROM mention_receipts WHERE origin=? AND seq=? "
                "ORDER BY received_at,recipient", (origin, seq))]

    def add_relay_receipt(self, origin: str, seq: int, recipient_id: str, nick: str,
                          mention: str, received_at: int) -> bool:
        if (len(origin) != 32 or len(recipient_id) != 32 or
                any(char not in "0123456789abcdefABCDEF" for char in origin + recipient_id) or
                not 1 <= seq <= 0xFFFFFFFF or received_at < 0 or not nick or
                len(nick.encode("utf-8")) > 16 or
                not all(char.isprintable() for char in nick) or
                not valid_mention_name(mention)):
            return False
        with self.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO receipt_relay(origin,seq,recipient_id,nick,mention,received_at) "
                "VALUES(?,?,?,?,?,?)", (origin, seq, recipient_id, nick, mention, received_at))
            self.db.commit()
            return True

    def claim_pending_receipts(self, now: int, limit: int = 20) -> list[dict]:
        """Return due receipts and persist bounded retry backoff before sending."""
        with self.lock:
            rows = self.db.execute(
                "SELECT origin,seq,recipient_id,nick,mention,received_at,attempts FROM receipt_relay "
                "WHERE delivered=0 AND next_attempt<=? ORDER BY received_at LIMIT ?",
                (now, limit)).fetchall()
            for row in rows:
                delay = min(900, 30 * (2 ** min(int(row["attempts"]), 5)))
                self.db.execute(
                    "UPDATE receipt_relay SET attempts=attempts+1,next_attempt=? "
                    "WHERE origin=? AND seq=? AND recipient_id=? AND mention=?",
                    (now + delay, row["origin"], row["seq"], row["recipient_id"],
                     row["mention"]))
            self.db.commit()
            return [dict(row) for row in rows]

    def mark_relay_receipt_delivered(self, origin: str, seq: int, recipient_id: str,
                                     mention: str | None = None) -> None:
        with self.lock:
            query = ("UPDATE receipt_relay SET delivered=1 WHERE origin=? AND seq=? "
                     "AND recipient_id=?")
            params = (origin, seq, recipient_id)
            if mention is not None:
                query += " AND mention=?"
                params += (mention,)
            self.db.execute(query, params)
            self.db.commit()

    def channels(self) -> list[str]:
        with self.lock:
            return [row[0] for row in self.db.execute(
                "SELECT name FROM channels UNION SELECT DISTINCT channel FROM messages "
                "ORDER BY 1")]

    def close(self) -> None:
        with self.lock:
            self.db.close()
