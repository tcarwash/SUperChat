"""Compact application protocol carried inside Data2G broadcast KISS frames."""
from __future__ import annotations

import json
import re

MAGIC = b"KC1"
MAX_FRAME = 220
MIN_CHUNK_BYTES = 120
MAX_MESSAGE_BYTES = 4096
MAX_PARTS = (MAX_MESSAGE_BYTES + MIN_CHUNK_BYTES - 1) // MIN_CHUNK_BYTES
PROOF_CHUNK_BYTES = 190
MAX_PROOF_BYTES = PROOF_CHUNK_BYTES * 255
IMAGE_CHUNK_BYTES = 139
MAX_THUMBNAIL_BYTES = 8 * 1024
MAX_CACHED_THUMBNAIL_BYTES = MAX_THUMBNAIL_BYTES
CHANNEL_NAME = re.compile(r"^#[A-Za-z0-9_-]{1,31}$")


def _message_chunk_limit(channel_bytes: int, nick_bytes: int, *, relay: bool = False) -> int:
    # KC1D has a 48-byte fixed header; addressed KC1R records add one TTL byte.
    return MAX_FRAME - 48 - channel_bytes - nick_bytes - int(relay)


def pack(record: dict) -> bytes:
    if record.get("t") == "q" and "h" in record:
        try:
            destination = bytes.fromhex(record["to"])
            requester = bytes.fromhex(record["from"])
            origin = bytes.fromhex(record["o"])
            values = [int(record.get(key, default)) for key, default in
                      (("a", 0), ("b", 0), ("d", 0), ("f", 0), ("u", 0))]
            ttl = int(record.get("ttl", 0))
            attempt = int(record.get("k", 0))
            fragments = record["h"]
            sequences = record.get("r", [])
            if (any(len(value) != 16 for value in (destination, requester, origin)) or
                    any(not 0 <= value <= 0xFFFFFFFF for value in values) or
                    not 0 <= ttl <= 255 or not 0 <= attempt <= 31 or
                    not isinstance(fragments, list) or
                    not isinstance(sequences, list) or len(fragments) > 8 or
                    len(sequences) != len(fragments) or
                    any(not isinstance(item, list) or len(item) != 2 or
                        type(item[0]) is not int or type(item[1]) is not int or
                        item[0] != sequences[index] or
                        not 1 <= item[0] <= 0xFFFFFFFF or
                        not 0 <= item[1] < (1 << MAX_PARTS)
                        for index, item in enumerate(fragments))):
                raise ValueError("invalid fragment repair request")
            wire = (MAGIC + b"H" + destination + requester + origin +
                    b"".join(value.to_bytes(4, "big") for value in values) +
                    bytes((int(bool(record.get("x"))) | (attempt << 1),
                           ttl, len(fragments))) +
                    b"".join(seq.to_bytes(4, "big") + mask.to_bytes(8, "big")
                             for seq, mask in fragments))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid fragment repair request") from exc
        if len(wire) > MAX_FRAME:
            raise ValueError(f"chat frame is too large ({len(wire)} > {MAX_FRAME} bytes)")
        return wire
    if record.get("t") == "r":
        try:
            origin = bytes.fromhex(record["o"])
            recipient = bytes.fromhex(record["p"])
            sender = bytes.fromhex(record["from"])
            seq, received_at = int(record["s"]), int(record["at"])
            if not isinstance(record["n"], str) or not isinstance(record["m"], str):
                raise ValueError("invalid mention receipt labels")
            nick, mention = record["n"].encode("utf-8"), record["m"].encode("utf-8")
            if (len(origin) != 16 or len(recipient) != 16 or len(sender) != 16 or
                    not 1 <= seq <= 0xFFFFFFFF or not 0 <= received_at <= 0xFFFFFFFF or
                    not 1 <= len(nick) <= 16 or not 1 <= len(mention) <= 16):
                raise ValueError("invalid mention receipt")
            return (MAGIC + b"r" + origin + seq.to_bytes(4, "big") + recipient +
                    sender + received_at.to_bytes(4, "big") +
                    bytes((len(nick), len(mention))) + nick + mention)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid mention receipt") from exc
    if record.get("t") == "y":
        try:
            destination = bytes.fromhex(record["p"])
            origin = bytes.fromhex(record["o"])
            digest = bytes.fromhex(record["h"])
            request_id = bytes.fromhex(record["id"])
            claimant = bytes.fromhex(record["by"])
            seq, ttl = int(record["s"]), int(record.get("ttl", 0))
            if (len(destination) != 16 or len(origin) != 16 or len(digest) != 32 or
                    len(request_id) != 4 or len(claimant) != 16 or
                    not 1 <= seq <= 0xFFFFFFFF or not 0 <= ttl <= 255):
                raise ValueError("invalid image claim")
            return (MAGIC + b"Y" + destination + origin + seq.to_bytes(4, "big") +
                    digest + request_id + claimant + bytes((ttl,)))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid image claim") from exc
    if record.get("t") == "e":
        try:
            identifiers = [bytes.fromhex(record[key]) for key in ("to", "from", "o")]
            values = [int(record.get(key, default)) for key, default in
                      (("a", 0), ("b", 0), ("u", record.get("m", 0)),
                       ("d", 0), ("f", record.get("a", 0)))]
            ttl = int(record.get("ttl", 0))
            sequences = record.get("g", [])
            attempt = int(record.get("k", 0))
            flags = (int(bool(record.get("x"))) |
                     (int(bool(record.get("v"))) << 1) |
                     (int(bool(record.get("j"))) << 2) |
                     (attempt << 3))
            if (any(len(value) != 16 for value in identifiers) or
                    any(not 0 <= value <= 0xFFFFFFFF for value in values) or
                    not 0 <= ttl <= 255 or not 0 <= attempt <= 31 or
                    not isinstance(sequences, list) or
                    len(sequences) > 8 or
                    any(type(seq) is not int or not 0 <= seq <= 0xFFFFFFFF
                        for seq in sequences)):
                raise ValueError("invalid page-end record")
            wire = (MAGIC + b"E" + b"".join(identifiers) +
                    b"".join(value.to_bytes(4, "big") for value in values) +
                    bytes((flags, ttl, len(sequences))) +
                    b"".join(seq.to_bytes(4, "big") for seq in sequences))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid page-end record") from exc
        if len(wire) > MAX_FRAME:
            raise ValueError(f"chat frame is too large ({len(wire)} > {MAX_FRAME} bytes)")
        return wire
    wire = MAGIC + b"J" + json.dumps(
        record, ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")
    if len(wire) > MAX_FRAME:
        raise ValueError(f"chat frame is too large ({len(wire)} > {MAX_FRAME} bytes)")
    return wire


def unpack(wire: bytes) -> dict | None:
    if not wire.startswith(MAGIC):
        return None
    if len(wire) > MAX_FRAME:
        return None
    if len(wire) < 4 or wire[3:4] != b"J":
        if wire[3:4] == b"H":
            if len(wire) < 75:
                return None
            destination = wire[4:20].hex()
            requester = wire[20:36].hex()
            origin = wire[36:52].hex()
            values = [int.from_bytes(wire[52 + index * 4:56 + index * 4], "big")
                      for index in range(5)]
            flags, ttl, count = wire[72], wire[73], wire[74]
            if flags & ~0x3F or count > 8 or len(wire) != 75 + count * 12:
                return None
            fragments = []
            offset = 75
            for _ in range(count):
                seq = int.from_bytes(wire[offset:offset + 4], "big")
                mask = int.from_bytes(wire[offset + 4:offset + 12], "big")
                if not seq or mask >= (1 << MAX_PARTS):
                    return None
                fragments.append([seq, mask])
                offset += 12
            return {"t": "q", "to": destination, "from": requester,
                    "o": origin, "a": values[0], "b": values[1],
                    "d": values[2], "f": values[3], "u": values[4],
                    "x": bool(flags & 1), "k": flags >> 1, "ttl": ttl,
                    "r": [item[0] for item in fragments], "h": fragments}
        if wire[3:4] == b"r":
            if len(wire) < 62:
                return None
            offset = 4
            origin = wire[offset:offset + 16].hex()
            seq = int.from_bytes(wire[offset + 16:offset + 20], "big")
            recipient = wire[offset + 20:offset + 36].hex()
            sender = wire[offset + 36:offset + 52].hex()
            received_at = int.from_bytes(wire[offset + 52:offset + 56], "big")
            nick_length, mention_length = wire[offset + 56:offset + 58]
            if (not seq or not nick_length or not mention_length or
                    nick_length > 16 or mention_length > 16 or
                    len(wire) != offset + 58 + nick_length + mention_length):
                return None
            try:
                nick = wire[offset + 58:offset + 58 + nick_length].decode("utf-8")
                mention = wire[offset + 58 + nick_length:].decode("utf-8")
            except UnicodeDecodeError:
                return None
            return {"t": "r", "to": origin, "o": origin, "s": seq,
                    "p": recipient, "from": sender, "n": nick,
                    "m": mention, "at": received_at}
        if wire[3:4] == b"Y":
            if len(wire) != 93:
                return None
            offset = 4
            destination = wire[offset:offset + 16].hex()
            origin = wire[offset + 16:offset + 32].hex()
            seq = int.from_bytes(wire[offset + 32:offset + 36], "big")
            digest = wire[offset + 36:offset + 68].hex()
            request_id = wire[offset + 68:offset + 72].hex()
            claimant = wire[offset + 72:offset + 88].hex()
            ttl = wire[offset + 88]
            if not seq:
                return None
            return {"t": "y", "p": destination, "o": origin, "s": seq,
                    "h": digest, "id": request_id, "by": claimant, "ttl": ttl}
        if wire[3:4] == b"E":
            if len(wire) < 75:
                return None
            flags, ttl, count = wire[72], wire[73], wire[74]
            if count > 8 or len(wire) != 75 + count * 4:
                return None
            offset = 4
            to = wire[offset:offset + 16].hex()
            sender = wire[offset + 16:offset + 32].hex()
            origin = wire[offset + 32:offset + 48].hex()
            offset += 48
            after, last, through, since, window_start = (
                int.from_bytes(wire[offset + index * 4:offset + (index + 1) * 4], "big")
                for index in range(5))
            offset += 20 + 3
            sequences = [int.from_bytes(wire[offset + index * 4:
                                              offset + (index + 1) * 4], "big")
                         for index in range(count)]
            return {"t": "e", "to": to, "from": sender, "o": origin,
                    "a": after, "b": last, "u": through, "d": since,
                    "f": window_start, "x": bool(flags & 1),
                    "v": bool(flags & 2), "j": bool(flags & 4),
                    "k": flags >> 3, "ttl": ttl, "g": sequences}
        if len(wire) >= 76 and wire[3:4] in {b"I", b"i"}:
            if wire[3:4] == b"i" and len(wire) < 81:
                return None
            dest, origin = wire[4:20], wire[20:36]
            seq = int.from_bytes(wire[36:40], "big")
            digest = wire[40:72].hex()
            index = int.from_bytes(wire[72:74], "big")
            count = int.from_bytes(wire[74:76], "big")
            ttl = wire[76] if wire[3:4] == b"i" else 0
            request_id = wire[77:81].hex() if wire[3:4] == b"i" else ""
            offset = 81 if wire[3:4] == b"i" else 76
            chunk_limit = IMAGE_CHUNK_BYTES if wire[3:4] == b"i" else 144
            if count and index < count and count <= 200 and len(wire[offset:]) <= chunk_limit:
                return {"t": "x", "to": dest.hex(), "o": origin.hex(), "s": seq,
                        "h": digest, "i": index, "z": count, "ttl": ttl,
                        "id": request_id,
                        "b": wire[offset:]}
            return None
        proof_relay = wire[3:4] == b"Q"
        if wire[3:4] == b"P" or proof_relay:
            start = 5 if proof_relay else 4
            if len(wire) < start + 22:
                return None
            ttl = wire[4] if proof_relay else 0
            origin = wire[start:start + 16].hex()
            seq = int.from_bytes(wire[start + 16:start + 20], "big")
            index, count = wire[start + 20], wire[start + 21]
            if count and index < count and len(wire[start + 22:]) <= PROOF_CHUNK_BYTES:
                return {"t": "p", "o": origin, "s": seq,
                        "i": index, "z": count, "ttl": ttl,
                        "b": wire[start + 22:]}
            return None
        relay = wire[3:4] == b"R"
        if (wire[3:4] != b"D" and not relay) or len(wire) < (49 if relay else 48):
            return None
        try:
            offset = 1 if relay else 0
            ttl = wire[4] if relay else 0
            start = 4 + offset
            dest = wire[start:start + 16]
            origin = wire[start + 16:start + 32]
            seq = int.from_bytes(wire[start + 32:start + 36], "big")
            timestamp = int.from_bytes(wire[start + 36:start + 40], "big")
            channel_len, nick_len, index, count = wire[start + 40:start + 44]
            adaptive = bool(count & 0x80)
            count &= 0x7F
            end = start + 44 + channel_len + nick_len
            channel = wire[start + 44:start + 44 + channel_len].decode("utf-8")
            nick = wire[start + 44 + channel_len:end].decode("utf-8")
            max_chunk = (_message_chunk_limit(channel_len, nick_len, relay=relay)
                         if adaptive else MIN_CHUNK_BYTES)
            chunk = wire[end:]
            if (channel_len > 32 or nick_len > 16 or count == 0 or index >= count or
                    end > len(wire) or not CHANNEL_NAME.fullmatch(channel) or not nick or
                    not all(char.isprintable() for char in nick) or
                    len(chunk) > max_chunk or
                    (index < count - 1 and len(chunk) != max_chunk)):
                return None
            return {"t": "d", "to": dest.hex() if any(dest) else "",
                    "o": origin.hex(), "s": seq, "ts": timestamp,
                    "i": index, "z": count, "c": channel, "n": nick,
                    "b": chunk, "ttl": ttl, "chunk": max_chunk,
                    "adaptive": adaptive}
        except (IndexError, UnicodeDecodeError):
            return None
    try:
        value = json.loads(wire[4:].decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def message_parts(*, origin: str, seq: int, channel: str, nick: str,
                  timestamp: int, text: str, to: str = "", ttl: int = 4) -> list[bytes]:
    body = text.encode("utf-8")
    if len(body) > MAX_MESSAGE_BYTES:
        raise ValueError(f"message exceeds {MAX_MESSAGE_BYTES} UTF-8 bytes")
    try:
        origin_bytes = bytes.fromhex(origin)
        dest_bytes = bytes.fromhex(to) if to else bytes(16)
    except ValueError as exc:
        raise ValueError("station IDs must be 32 hexadecimal characters") from exc
    if len(origin_bytes) != 16 or len(dest_bytes) != 16:
        raise ValueError("station IDs must be 128-bit values")
    if not 0 <= ttl <= 255:
        raise ValueError("relay limit is outside the wire range")
    channel_bytes, nick_bytes = channel.encode("utf-8"), nick.encode("utf-8")
    if (not CHANNEL_NAME.fullmatch(channel) or not nick or
            len(channel_bytes) > 32 or len(nick_bytes) > 16 or
            not all(char.isprintable() for char in nick)):
        raise ValueError("channel is limited to 32 UTF-8 bytes and nick to 16")
    if not all(char.isprintable() or char in "\t\n" for char in text):
        raise ValueError("message contains terminal control characters")
    if not 1 <= seq <= 0xFFFFFFFF or not 0 <= timestamp <= 0xFFFFFFFF:
        raise ValueError("sequence or timestamp is outside the wire range")
    relay = bool(to)
    chunk_limit = _message_chunk_limit(len(channel_bytes), len(nick_bytes), relay=relay)
    if chunk_limit < MIN_CHUNK_BYTES:
        raise ValueError("channel and nick leave too little room in a radio frame")
    chunks = [body[i:i + chunk_limit] for i in range(0, len(body), chunk_limit)] or [b""]
    if len(chunks) > MAX_PARTS:
        raise ValueError("message has too many fragments")
    encoded = []
    for index, chunk in enumerate(chunks):
        count = len(chunks) | 0x80
        encoded.append(MAGIC + (b"R" if relay else b"D") +
                       (bytes((ttl,)) if relay else b"") + dest_bytes + origin_bytes +
                       seq.to_bytes(4, "big") + timestamp.to_bytes(4, "big") +
                       bytes((len(channel_bytes), len(nick_bytes), index, count)) +
                       channel_bytes + nick_bytes + chunk)
    return encoded


def relay_message_part(part: dict, ttl: int) -> bytes:
    """Re-encode one addressed history fragment without changing its contents."""
    recipient, origin = bytes.fromhex(part["to"]), bytes.fromhex(part["o"])
    if len(recipient) != 16 or len(origin) != 16 or not 0 <= ttl <= 255:
        raise ValueError("invalid addressed history fragment")
    seq, timestamp = int(part["s"]), int(part["ts"])
    channel, nick = part["c"].encode("utf-8"), part["n"].encode("utf-8")
    index, count, body = int(part["i"]), int(part["z"]), part["b"]
    adaptive = bool(part.get("adaptive", False))
    chunk_limit = (_message_chunk_limit(len(channel), len(nick), relay=True)
                   if adaptive else MIN_CHUNK_BYTES)
    if (not 1 <= seq <= 0xFFFFFFFF or not 0 <= timestamp <= 0xFFFFFFFF or
            not CHANNEL_NAME.fullmatch(part["c"]) or not 1 <= len(nick) <= 16 or
            len(channel) > 32 or not 1 <= count <= MAX_PARTS or
            not 0 <= index < count or not isinstance(body, bytes) or
            len(body) > chunk_limit or (index < count - 1 and len(body) != chunk_limit)):
        raise ValueError("invalid addressed history fragment")
    wire_count = count | 0x80 if adaptive else count
    frame = (MAGIC + b"R" + bytes((ttl,)) + recipient + origin +
             seq.to_bytes(4, "big") + timestamp.to_bytes(4, "big") +
             bytes((len(channel), len(nick), index, wire_count)) + channel + nick + body)
    if len(frame) > MAX_FRAME:
        raise ValueError("addressed history fragment exceeds the frame limit")
    return frame


def proof_parts(*, origin: str, seq: int, proof: bytes, ttl: int = 4) -> list[bytes]:
    """Encode a detached ARAP proof in independently reassemblable KISS frames."""
    if len(proof) > MAX_PROOF_BYTES:
        raise ValueError(f"ARAP proof exceeds the wire limit of {MAX_PROOF_BYTES} bytes")
    origin_bytes = bytes.fromhex(origin)
    if (len(origin_bytes) != 16 or not 1 <= seq <= 0xFFFFFFFF or
            not 0 <= ttl <= 255):
        raise ValueError("invalid proof message identity")
    chunks = [proof[i:i + PROOF_CHUNK_BYTES]
              for i in range(0, len(proof), PROOF_CHUNK_BYTES)] or [b""]
    if len(chunks) > 255:
        raise ValueError("ARAP proof requires too many frames")
    return [MAGIC + b"Q" + bytes((ttl,)) + origin_bytes + seq.to_bytes(4, "big") +
            bytes((i, len(chunks))) + chunk for i, chunk in enumerate(chunks)]


def relay_proof_part(part: dict, ttl: int) -> bytes:
    """Re-encode one detached proof fragment with a decremented mesh hop limit."""
    origin = bytes.fromhex(part["o"])
    seq, index, count, body = (int(part["s"]), int(part["i"]), int(part["z"]), part["b"])
    if (len(origin) != 16 or not 1 <= seq <= 0xFFFFFFFF or
            not 0 <= ttl <= 255 or not 1 <= count <= 255 or
            not 0 <= index < count or not isinstance(body, bytes) or
            len(body) > PROOF_CHUNK_BYTES):
        raise ValueError("invalid proof fragment")
    return (MAGIC + b"Q" + bytes((ttl,)) + origin + seq.to_bytes(4, "big") +
            bytes((index, count)) + body)


def image_part(*, recipient: str, origin: str, seq: int, digest: str,
               index: int, count: int, chunk: bytes, request_id: str = "00000000",
               ttl: int = 4) -> bytes:
    dest, source, sha = bytes.fromhex(recipient), bytes.fromhex(origin), bytes.fromhex(digest)
    if len(dest) != 16 or len(source) != 16 or len(sha) != 32:
        raise ValueError("invalid image identity")
    if (not 1 <= seq <= 0xFFFFFFFF or
            len(request_id) != 8 or any(char not in "0123456789abcdefABCDEF"
                                    for char in request_id) or
            not 1 <= count <= 200 or not 0 <= index < count or not 0 <= ttl <= 255 or
            len(chunk) > IMAGE_CHUNK_BYTES):
        raise ValueError("invalid image fragment")
    return (MAGIC + b"i" + dest + source + seq.to_bytes(4, "big") + sha +
            index.to_bytes(2, "big") + count.to_bytes(2, "big") + bytes((ttl,)) +
            bytes.fromhex(request_id) + chunk)


def legacy_image_part(*, recipient: str, origin: str, seq: int, digest: str,
                      index: int, count: int, chunk: bytes) -> bytes:
    """Encode an addressed image fragment for peers using the original format."""
    dest, source, sha = bytes.fromhex(recipient), bytes.fromhex(origin), bytes.fromhex(digest)
    if (len(dest) != 16 or len(source) != 16 or len(sha) != 32 or
            not 1 <= seq <= 0xFFFFFFFF or not 1 <= count <= 200 or
            not 0 <= index < count or len(chunk) > 144):
        raise ValueError("invalid legacy image fragment")
    return (MAGIC + b"I" + dest + source + seq.to_bytes(4, "big") + sha +
            index.to_bytes(2, "big") + count.to_bytes(2, "big") + chunk)


def image_parts(*, recipient: str, origin: str, seq: int, digest: str,
                data: bytes, request_id: str = "00000000", ttl: int = 4,
                indexes: list[int] | None = None) -> list[bytes]:
    """Target thumbnail bytes to one requesting station, in small radio frames."""
    if len(data) > MAX_CACHED_THUMBNAIL_BYTES:
        raise ValueError("thumbnail exceeds 8 KiB")
    legacy = request_id == "legacy"
    chunk_size = 144 if legacy else IMAGE_CHUNK_BYTES
    chunks = [data[i:i + chunk_size]
              for i in range(0, len(data), chunk_size)] or [b""]
    if len(chunks) > 200:
        raise ValueError("thumbnail requires too many fragments")
    selected = range(len(chunks)) if indexes is None else indexes
    if any(not isinstance(i, int) or not 0 <= i < len(chunks) for i in selected):
        raise ValueError("requested image fragment is outside the thumbnail")
    if legacy:
        return [legacy_image_part(recipient=recipient, origin=origin, seq=seq,
                                  digest=digest, index=i, count=len(chunks),
                                  chunk=chunks[i]) for i in selected]
    return [image_part(recipient=recipient, origin=origin, seq=seq, digest=digest,
                       index=i, count=len(chunks), chunk=chunks[i],
                       request_id=request_id, ttl=ttl) for i in selected]
