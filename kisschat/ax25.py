"""Small AX.25 UI frame codec for carrying KISSChat records over raw KISS."""
from __future__ import annotations

import re
import zlib

_ADDRESS = re.compile(r"^([A-Z0-9]{1,6})(?:-([0-9]{1,2}))?$")
MAX_INFORMATION_BYTES = 256
_BATCH_MAGIC = b"KC1B"


def _address(value: str, *, last: bool, repeated: bool = False) -> bytes:
    match = _ADDRESS.fullmatch(value.upper())
    if not match:
        raise ValueError(f"invalid AX.25 address {value!r}; use CALL or CALL-SSID")
    call, ssid_text = match.groups()
    ssid = int(ssid_text or 0)
    if ssid > 15:
        raise ValueError("AX.25 SSID must be between 0 and 15")
    padded = call.ljust(6)
    encoded = bytearray(ord(char) << 1 for char in padded)
    # AX.25 uses SSID-octet bit 0 to mark the end of the address list;
    # bit 7 is the repeater H bit (or C/R bit on destination/source).
    encoded.append(0x60 | (ssid << 1) | (0x80 if repeated else 0) |
                  (0x01 if last else 0))
    return bytes(encoded)


def encode_ui_frame(source: str, destination: str, path: tuple[str, ...], info: bytes) -> bytes:
    """Build a no-layer-3 AX.25 UI frame; KISS/TNC adds HDLC FCS on air."""
    if len(path) > 8:
        raise ValueError("AX.25 digipeater path is limited to 8 stations")
    addresses = [_address(destination, last=False),
                _address(source, last=not path)]
    addresses.extend(_address(hop, last=index == len(path) - 1)
                     for index, hop in enumerate(path))
    return b"".join(addresses) + b"\x03\xf0" + info


def decode_ui_frame(frame: bytes) -> bytes | None:
    """Return UI information payload, or None for non-UI/non-chat PID frames."""
    offset = 0
    addresses = 0
    while True:
        if offset + 7 > len(frame) or addresses >= 10:
            return None
        extension = frame[offset + 6] & 1
        offset += 7
        addresses += 1
        if extension:
            break
    if len(frame) < offset + 2 or frame[offset:offset + 2] != b"\x03\xf0":
        return None
    return frame[offset + 2:]


def pack_batch(payloads: tuple[bytes, ...] | list[bytes], *,
               max_bytes: int = MAX_INFORMATION_BYTES) -> bytes | None:
    """Bundle and compress short chat records into one radio frame.

    Returns None when there is only one record or the bundle exceeds the
    caller's radio-frame payload limit.
    """
    if len(payloads) < 2 or len(payloads) > 255:
        return None
    raw = bytearray()
    for payload in payloads:
        if not payload.startswith(b"KC1") or len(payload) > 255:
            return None
        raw.append(len(payload))
        raw.extend(payload)
    compressed = zlib.compress(raw, level=9)
    if len(compressed) < len(raw):
        flags, body = 1, compressed
    else:
        flags, body = 0, bytes(raw)
    result = (_BATCH_MAGIC + bytes((flags, len(payloads))) +
              len(raw).to_bytes(2, "big") + body)
    return result if len(result) <= max_bytes else None


def unpack_batch(info: bytes) -> tuple[bytes, ...] | None:
    """Unwrap a valid KISSChat AX.25 bundle; leave ordinary records untouched."""
    if not info.startswith(_BATCH_MAGIC) or len(info) < len(_BATCH_MAGIC) + 4:
        return None
    flags, count = info[4], info[5]
    expected = int.from_bytes(info[6:8], "big")
    if (flags not in (0, 1) or count < 2 or
            not count <= expected <= count * 256):
        return None
    body = info[8:]
    if flags:
        try:
            decoder = zlib.decompressobj()
            raw = decoder.decompress(body, expected + 1)
            if (len(raw) != expected or not decoder.eof or decoder.unused_data or
                    decoder.unconsumed_tail):
                return None
        except zlib.error:
            return None
    else:
        if len(body) != expected:
            return None
        raw = body
    offset = 0
    records = []
    for _ in range(count):
        if offset >= len(raw):
            return None
        length = raw[offset]
        offset += 1
        if length == 0 or offset + length > len(raw):
            return None
        record = raw[offset:offset + length]
        if not record.startswith(b"KC1") or record.startswith(_BATCH_MAGIC):
            return None
        records.append(record)
        offset += length
    if offset != len(raw):
        return None
    return tuple(records)
