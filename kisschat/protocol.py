"""Compact application protocol carried inside Data2G broadcast KISS frames."""
from __future__ import annotations

import re

MAGIC = b"KC2"
MAX_FRAME = 220
MAX_MESSAGE_BYTES = 4096
MIN_CHUNK_BYTES = 92
MAX_PARTS = (MAX_MESSAGE_BYTES + MIN_CHUNK_BYTES - 1) // MIN_CHUNK_BYTES
MAX_SPARSE_SEQUENCE_REQUESTS = (MAX_FRAME - 75) // 4
PROOF_CHUNK_BYTES = 190
MAX_PROOF_BYTES = PROOF_CHUNK_BYTES * 255
IMAGE_CHUNK_BYTES = 139
MAX_THUMBNAIL_BYTES = 8 * 1024
MAX_CACHED_THUMBNAIL_BYTES = MAX_THUMBNAIL_BYTES
CHANNEL_NAME = re.compile(r"^#[A-Za-z0-9_-]{1,31}$")
_KEYS = ("t", "p", "n", "nick", "v", "l", "ttl", "sig", "o", "a", "m",
         "to", "from", "s", "h", "b", "w", "hgt", "id", "i", "z", "c",
         "ts", "d", "f", "u", "x", "r", "k", "g", "at", "by", "q",
         "j", "proof", "version", "channel", "digest", "size", "width",
         "height", "requester", "recipient", "attempt", "since", "through")
_KEY_TO_ID = {key: index + 1 for index, key in enumerate(_KEYS)}
_ID_TO_KEY = {value: key for key, value in _KEY_TO_ID.items()}
_ID_TEXT = re.compile(r"^[0-9a-fA-F]{32}$")
_DIGEST_TEXT = re.compile(r"^[0-9a-fA-F]{64}$")


def _put_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("negative varint")
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _get_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = shift = 0
    for _ in range(32):
        if offset >= len(data):
            raise ValueError("truncated varint")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
    raise ValueError("oversized varint")


def _encode_value(value, depth: int = 0) -> bytes:
    if depth > 8:
        raise ValueError("record nesting is too deep")
    if value is None:
        return b"\x00"
    if value is False:
        return b"\x01"
    if value is True:
        return b"\x02"
    if type(value) is int:
        zigzag = value * 2 if value >= 0 else (-value * 2) - 1
        return b"\x03" + _put_varint(zigzag)
    if isinstance(value, bytes):
        return b"\x05" + _put_varint(len(value)) + value
    if isinstance(value, str):
        if _ID_TEXT.fullmatch(value):
            return b"\x08" + bytes.fromhex(value)
        if _DIGEST_TEXT.fullmatch(value):
            return b"\x09" + bytes.fromhex(value)
        encoded = value.encode("utf-8")
        return b"\x04" + _put_varint(len(encoded)) + encoded
    if isinstance(value, (list, tuple)):
        return b"\x06" + _put_varint(len(value)) + b"".join(
            _encode_value(item, depth + 1) for item in value)
    if isinstance(value, dict):
        return b"\x07" + _encode_map(value, depth + 1)
    raise ValueError(f"unsupported protocol value: {type(value).__name__}")


def _encode_map(value: dict, depth: int = 0) -> bytes:
    if depth > 8 or len(value) > 255:
        raise ValueError("record map is too large or nested too deeply")
    out = bytearray((len(value),))
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("protocol map keys must be strings")
        key_bytes = key.encode("utf-8")
        code = _KEY_TO_ID.get(key)
        if code is None:
            out.append(0)
            out.extend(_put_varint(len(key_bytes)))
            out.extend(key_bytes)
        else:
            out.append(code)
        out.extend(_encode_value(item, depth))
    return bytes(out)


def _decode_value(data: bytes, offset: int, depth: int = 0):
    if depth > 8 or offset >= len(data):
        raise ValueError("truncated or deeply nested value")
    kind = data[offset]
    offset += 1
    if kind == 0:
        return None, offset
    if kind == 1:
        return False, offset
    if kind == 2:
        return True, offset
    if kind == 3:
        value, offset = _get_varint(data, offset)
        return (-(value // 2) - 1 if value & 1 else value // 2), offset
    if kind in (4, 5):
        size, offset = _get_varint(data, offset)
        end = offset + size
        if end > len(data):
            raise ValueError("truncated string or bytes")
        raw = data[offset:end]
        return (raw.decode("utf-8") if kind == 4 else raw), end
    if kind in (8, 9):
        size = 16 if kind == 8 else 32
        end = offset + size
        if end > len(data):
            raise ValueError("truncated identifier")
        return data[offset:end].hex(), end
    if kind == 6:
        count, offset = _get_varint(data, offset)
        if count > MAX_FRAME:
            raise ValueError("oversized list")
        values = []
        for _ in range(count):
            item, offset = _decode_value(data, offset, depth + 1)
            values.append(item)
        return values, offset
    if kind == 7:
        mapping, offset = _decode_map(data, offset, depth + 1)
        return mapping, offset
    raise ValueError("unknown value type")


def _decode_map(data: bytes, offset: int = 0, depth: int = 0) -> tuple[dict, int]:
    if depth > 8 or offset >= len(data):
        raise ValueError("truncated or deeply nested map")
    count = data[offset]
    offset += 1
    result = {}
    for _ in range(count):
        if offset >= len(data):
            raise ValueError("truncated map key")
        code = data[offset]
        offset += 1
        if code == 0:
            size, offset = _get_varint(data, offset)
            end = offset + size
            if end > len(data):
                raise ValueError("truncated map key")
            key = data[offset:end].decode("utf-8")
            offset = end
        else:
            key = _ID_TO_KEY.get(code)
            if key is None:
                raise ValueError("unknown map key")
        if key in result:
            raise ValueError("duplicate map key")
        result[key], offset = _decode_value(data, offset, depth + 1)
    return result, offset


def _message_chunk_limit(channel_bytes: int, nick_bytes: int, *, relay: bool = False,
                         sender: bool = False) -> int:
    # KC2D has a 48-byte fixed header; addressed records add a TTL byte and
    # sender-tagged addressed records add a node ID (direct records reuse dest).
    return (MAX_FRAME - 48 - channel_bytes - nick_bytes - int(relay) -
            (16 if sender else 0))


def pack(record: dict) -> bytes:
    if record.get("t") == "k":
        try:
            sender = bytes.fromhex(record["from"])
            entries = record.get("h", [])
            if (len(sender) != 16 or not isinstance(entries, list) or
                    len(entries) > 8 or
                    any(not isinstance(item, list) or len(item) != 3 or
                        not isinstance(item[0], str) or len(bytes.fromhex(item[0])) != 16 or
                        any(type(value) is not int or not 0 <= value <= 0xFFFFFFFF
                            for value in item[1:]) or item[1] > item[2]
                        for item in entries)):
                raise ValueError("invalid history summary")
            return (MAGIC + b"K" + sender + bytes((len(entries),)) +
                    b"".join(bytes.fromhex(origin) + low.to_bytes(4, "big") +
                             high.to_bytes(4, "big")
                             for origin, low, high in entries))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid history summary") from exc
    if record.get("t") == "q" and "r" in record and "h" not in record:
        try:
            destination = bytes.fromhex(record["to"])
            requester = bytes.fromhex(record["from"])
            origin = bytes.fromhex(record["o"])
            values = [int(record.get(key, default)) for key, default in
                      (("a", 0), ("b", 0), ("d", 0), ("f", 0), ("u", 0))]
            ttl = int(record.get("ttl", 0))
            attempt = int(record.get("k", 0))
            sequences = record["r"]
            if (any(len(value) != 16 for value in (destination, requester, origin)) or
                    any(not 0 <= value <= 0xFFFFFFFF for value in values) or
                    not 0 <= ttl <= 255 or not 0 <= attempt <= 31 or
                    not isinstance(sequences, list) or not sequences or
                    len(sequences) > MAX_SPARSE_SEQUENCE_REQUESTS or
                    any(type(seq) is not int or not values[0] < seq <= values[1]
                        for seq in sequences) or
                    any(left >= right for left, right in zip(sequences, sequences[1:]))):
                raise ValueError("invalid sparse history request")
            flags = (int(bool(record.get("x"))) | (attempt << 1) |
                     (int(bool(record.get("j"))) << 6))
            wire = (MAGIC + b"S" + destination + requester + origin +
                    b"".join(value.to_bytes(4, "big") for value in values) +
                    bytes((flags, ttl, len(sequences))) +
                    b"".join(seq.to_bytes(4, "big") for seq in sequences))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid sparse history request") from exc
        if len(wire) > MAX_FRAME:
            raise ValueError(f"chat frame is too large ({len(wire)} > {MAX_FRAME} bytes)")
        return wire
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
            page_limit = MAX_SPARSE_SEQUENCE_REQUESTS if record.get("j") else 8
            if (any(len(value) != 16 for value in identifiers) or
                    any(not 0 <= value <= 0xFFFFFFFF for value in values) or
                    not 0 <= ttl <= 255 or not 0 <= attempt <= 31 or
                    not isinstance(sequences, list) or
                    len(sequences) > page_limit or
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
    wire = MAGIC + b"M" + _encode_map(record)
    if len(wire) > MAX_FRAME:
        raise ValueError(f"chat frame is too large ({len(wire)} > {MAX_FRAME} bytes)")
    return wire


def unpack(wire: bytes) -> dict | None:
    if not wire.startswith(MAGIC):
        return None
    if len(wire) > MAX_FRAME:
        return None
    if len(wire) >= 5 and wire[3:4] == b"M":
        try:
            value, offset = _decode_map(wire, 4)
            return value if offset == len(wire) and isinstance(value, dict) else None
        except (UnicodeDecodeError, ValueError):
            return None
    if len(wire) < 4 or wire[3:4] != b"M":
        if wire[3:4] == b"K":
            if len(wire) < 21 or wire[20] > 8 or len(wire) != 21 + wire[20] * 24:
                return None
            sender = wire[4:20].hex()
            entries = []
            offset = 21
            for _ in range(wire[20]):
                origin = wire[offset:offset + 16].hex()
                low = int.from_bytes(wire[offset + 16:offset + 20], "big")
                high = int.from_bytes(wire[offset + 20:offset + 24], "big")
                if not low or low > high:
                    return None
                entries.append([origin, low, high])
                offset += 24
            return {"t": "k", "from": sender, "h": entries}
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
        if wire[3:4] == b"S":
            if len(wire) < 76:
                return None
            destination = wire[4:20].hex()
            requester = wire[20:36].hex()
            origin = wire[36:52].hex()
            values = [int.from_bytes(wire[52 + index * 4:56 + index * 4], "big")
                      for index in range(5)]
            flags, ttl, count = wire[72], wire[73], wire[74]
            if (flags & ~0x7F or not 1 <= count <= MAX_SPARSE_SEQUENCE_REQUESTS or
                    len(wire) != 75 + count * 4):
                return None
            sequences = [int.from_bytes(wire[offset:offset + 4], "big")
                         for offset in range(75, len(wire), 4)]
            if (any(not values[0] < seq <= values[1] for seq in sequences) or
                    any(left >= right for left, right in zip(sequences, sequences[1:]))):
                return None
            return {"t": "q", "to": destination, "from": requester,
                    "o": origin, "a": values[0], "b": values[1],
                    "d": values[2], "f": values[3], "u": values[4],
                    "x": bool(flags & 1), "k": (flags >> 1) & 0x1F,
                    "j": bool(flags & 0x40), "ttl": ttl, "r": sequences}
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
            page_limit = MAX_SPARSE_SEQUENCE_REQUESTS if flags & 4 else 8
            if count > page_limit or len(wire) != 75 + count * 4:
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
        tag = wire[3:4]
        relay = tag in {b"R", b"V"}
        sender_tagged = tag in {b"d", b"V"}
        if tag not in {b"D", b"R", b"d", b"V"}:
            return None
        sender_extra = sender_tagged and relay
        minimum = 48 + int(relay) + (16 if sender_extra else 0)
        if len(wire) < minimum:
            return None
        try:
            offset = 1 if relay else 0
            ttl = wire[4] if relay else 0
            start = 4 + offset
            dest = wire[start:start + 16]
            sender_start = start + 16
            sender_id = (wire[sender_start:sender_start + 16] if sender_extra else
                         dest if sender_tagged else b"")
            origin_start = sender_start + (16 if sender_extra else 0)
            origin = wire[origin_start:origin_start + 16]
            seq = int.from_bytes(wire[origin_start + 16:origin_start + 20], "big")
            timestamp = int.from_bytes(wire[origin_start + 20:origin_start + 24], "big")
            channel_len, nick_len, index, count = wire[origin_start + 24:origin_start + 28]
            adaptive = bool(count & 0x80)
            count &= 0x7F
            end = origin_start + 28 + channel_len + nick_len
            channel = wire[origin_start + 28:origin_start + 28 + channel_len].decode("utf-8")
            nick = wire[origin_start + 28 + channel_len:end].decode("utf-8")
            max_chunk = (_message_chunk_limit(channel_len, nick_len, relay=relay,
                                              sender=sender_extra)
                         if adaptive else MIN_CHUNK_BYTES)
            chunk = wire[end:]
            if (channel_len > 32 or nick_len > 16 or count == 0 or index >= count or
                    end > len(wire) or not CHANNEL_NAME.fullmatch(channel) or not nick or
                    not all(char.isprintable() for char in nick) or
                    len(chunk) > max_chunk or
                    (index < count - 1 and len(chunk) != max_chunk)):
                return None
            record = {"t": "d", "to": dest.hex() if relay and any(dest) else "",
                      "o": origin.hex(), "s": seq, "ts": timestamp,
                      "i": index, "z": count, "c": channel, "n": nick,
                      "b": chunk, "ttl": ttl, "chunk": max_chunk,
                      "adaptive": adaptive}
            if sender_tagged:
                record["from"] = sender_id.hex()
            return record
        except (IndexError, UnicodeDecodeError):
            return None
    return None


def message_parts(*, origin: str, seq: int, channel: str, nick: str,
                  timestamp: int, text: str, to: str = "", ttl: int = 4,
                  sender: str | None = None) -> list[bytes]:
    body = text.encode("utf-8")
    if len(body) > MAX_MESSAGE_BYTES:
        raise ValueError(f"message exceeds {MAX_MESSAGE_BYTES} UTF-8 bytes")
    try:
        origin_bytes = bytes.fromhex(origin)
        dest_bytes = bytes.fromhex(to) if to else bytes(16)
        sender_bytes = bytes.fromhex(sender) if sender is not None else b""
    except ValueError as exc:
        raise ValueError("station IDs must be 32 hexadecimal characters") from exc
    if (len(origin_bytes) != 16 or len(dest_bytes) != 16 or
            (sender is not None and len(sender_bytes) != 16)):
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
    source_tagged = sender is not None
    sender_extra = source_tagged and relay
    chunk_limit = _message_chunk_limit(len(channel_bytes), len(nick_bytes),
                                      relay=relay, sender=sender_extra)
    if chunk_limit < MIN_CHUNK_BYTES:
        raise ValueError("channel and nick leave too little room in a radio frame")
    chunks = [body[i:i + chunk_limit] for i in range(0, len(body), chunk_limit)] or [b""]
    if len(chunks) > MAX_PARTS:
        raise ValueError("message has too many fragments")
    encoded = []
    for index, chunk in enumerate(chunks):
        count = len(chunks) | 0x80
        tag = ((b"V" if relay else b"d") if source_tagged else
               (b"R" if relay else b"D"))
        encoded.append(MAGIC + tag + (bytes((ttl,)) if relay else b"") +
                       (dest_bytes if relay or not source_tagged else sender_bytes) +
                       (sender_bytes if sender_extra else b"") + origin_bytes +
                       seq.to_bytes(4, "big") + timestamp.to_bytes(4, "big") +
                       bytes((len(channel_bytes), len(nick_bytes), index, count)) +
                       channel_bytes + nick_bytes + chunk)
    return encoded


def relay_message_part(part: dict, ttl: int, *, sender: str | None = None) -> bytes:
    """Re-encode one addressed history fragment without changing its contents."""
    recipient, origin = bytes.fromhex(part["to"]), bytes.fromhex(part["o"])
    sender_bytes = bytes.fromhex(sender) if sender is not None else b""
    if (len(recipient) != 16 or len(origin) != 16 or
            (sender is not None and len(sender_bytes) != 16) or
            not 0 <= ttl <= 255):
        raise ValueError("invalid addressed history fragment")
    seq, timestamp = int(part["s"]), int(part["ts"])
    channel, nick = part["c"].encode("utf-8"), part["n"].encode("utf-8")
    index, count, body = int(part["i"]), int(part["z"]), part["b"]
    adaptive = bool(part.get("adaptive", False))
    source_tagged = sender is not None
    chunk_limit = (_message_chunk_limit(len(channel), len(nick), relay=True,
                                        sender=source_tagged)
                   if adaptive else MIN_CHUNK_BYTES)
    if (not 1 <= seq <= 0xFFFFFFFF or not 0 <= timestamp <= 0xFFFFFFFF or
            not CHANNEL_NAME.fullmatch(part["c"]) or not 1 <= len(nick) <= 16 or
            len(channel) > 32 or not 1 <= count <= MAX_PARTS or
            not 0 <= index < count or not isinstance(body, bytes) or
            len(body) > chunk_limit or (index < count - 1 and len(body) != chunk_limit)):
        raise ValueError("invalid addressed history fragment")
    wire_count = count | 0x80 if adaptive else count
    frame = (MAGIC + (b"V" if source_tagged else b"R") + bytes((ttl,)) +
             recipient + sender_bytes + origin +
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
