"""Read, encode, and render PixelQSO pixel cards as first-class chat messages."""
from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import time
import zlib

from rich.text import Text

from .protocol import MAX_CACHED_THUMBNAIL_BYTES, MAX_MESSAGE_BYTES

CARD_MESSAGE_PREFIX = "KISSCHAT-CARD/1:"
ANIMATION_MESSAGE_PREFIX = "KISSCHAT-ANIMATION/1:"
CARD_REFERENCE_PREFIX = "KISSCHAT-CARD-REF/1:"
_CARD_MAGIC = b"KPC1"
_ANIMATION_MAGIC = b"KPA1"
# magic, dimensions, palette size, card ID, callsign/grid/title lengths, CRC32
_HEADER = struct.Struct(">4sBBBHBBBI")
_ANIMATION_HEADER = struct.Struct(">4sHH8sHHI")
_PALETTE_SIZES = {2, 8, 16, 32}
_ANIMATION_PALETTES = {
    8: ((0, 0, 0), (15, 15, 15), (0, 0, 15), (0, 15, 15),
        (0, 15, 0), (15, 15, 0), (15, 0, 0), (15, 0, 15)),
    16: tuple((r, g, b) for r in (0, 5, 10, 15) for g in (0, 15) for b in (0, 15)),
    32: tuple((r, g, b) for r in (0, 5, 10, 15)
              for g in (0, 5, 10, 15) for b in (0, 15)),
}
_MAX_DIMENSION = 64
_MAX_TITLE_BYTES = 48
_MAX_CARD_FILE_BYTES = 2 * 1024 * 1024
_MAX_LIBRARY_CARDS = 512


@dataclass(frozen=True)
class PixelCard:
    title: str
    callsign: str
    grid: str
    card_id: int
    width: int
    height: int
    palette: tuple[tuple[int, int, int], ...]
    pixels: tuple[int, ...]
    path: Path | None = None
    category: str = ""
    animated: bool = False
    animation_link: dict | None = None


@dataclass(frozen=True)
class PixelCardAnimation:
    title: str
    first: PixelCard
    second: PixelCard
    frame_ms: int
    first_digest: str
    path: Path | None = None
    category: str = ""

    @property
    def frames(self) -> tuple[PixelCard, PixelCard]:
        return self.first, self.second

    @property
    def callsign(self) -> str:
        return self.second.callsign

    @property
    def grid(self) -> str:
        return self.second.grid

    @property
    def card_id(self) -> int:
        return self.second.card_id

    @property
    def width(self) -> int:
        return self.second.width

    @property
    def height(self) -> int:
        return self.second.height

    @property
    def palette(self) -> tuple[tuple[int, int, int], ...]:
        return self.second.palette

    @property
    def pixels(self) -> tuple[int, ...]:
        return self.second.pixels

    @property
    def animated(self) -> bool:
        return True

    def frame_index(self, time_ms: int | float) -> int:
        return int(time_ms // self.frame_ms) % 2

    def frame_at(self, time_ms: int | float) -> PixelCard:
        return self.frames[self.frame_index(time_ms)]


@dataclass(frozen=True)
class PixelCardSummary:
    title: str
    callsign: str
    grid: str
    card_id: int
    width: int
    height: int
    palette: tuple[tuple[int, int, int], ...]
    animation_link: dict | None = None


@dataclass(frozen=True)
class PixelCardReference:
    digest: str
    size: int
    frames: tuple[PixelCardSummary, ...]
    frame_ms: int | None = None

    @property
    def is_animation(self) -> bool:
        return len(self.frames) == 2

    @property
    def is_linked_frame(self) -> bool:
        return len(self.frames) == 1 and self.frames[0].animation_link is not None

    @property
    def title(self) -> str:
        return (f"{self.frames[0].title} → {self.frames[1].title}"
                if self.is_animation else self.frames[0].title)

    @property
    def callsign(self) -> str:
        return self.frames[-1].callsign

    @property
    def grid(self) -> str:
        return self.frames[-1].grid

    @property
    def card_id(self) -> int:
        return self.frames[-1].card_id

    @property
    def width(self) -> int:
        return self.frames[-1].width

    @property
    def height(self) -> int:
        return self.frames[-1].height

    @property
    def palette(self) -> tuple[tuple[int, int, int], ...]:
        return self.frames[-1].palette


def _validate_animation_link(value: object, own_id: int) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("PixelQSO animation reference must be an object")
    first_id = value.get("first_card_id")
    frame_ms = value.get("frame_ms", 500)
    digest = value.get("first_digest")
    if (type(first_id) is not int or not 0 <= first_id <= 0xFFFF or
            first_id == own_id or type(frame_ms) is not int or
            not 100 <= frame_ms <= 10000 or not isinstance(digest, str) or
            len(digest) != 16 or any(ch not in "0123456789abcdefABCDEF" for ch in digest)):
        raise ValueError("PixelQSO animation reference is invalid")
    return {"first_card_id": first_id, "first_digest": digest.lower(),
            "frame_ms": frame_ms}


def pixelqso_artwork_digest(card: PixelCard) -> str:
    """Match PixelQSO's shared-avatar-palette animation fingerprint."""
    target = _ANIMATION_PALETTES.get(len(card.palette))
    if target is None:
        raise ValueError("PixelQSO animations require an 8-, 16-, or 32-color palette")
    colors = [card.palette[index] for index in card.pixels]
    mapping = {color: min(range(len(target)), key=lambda index:
                          sum((color[channel] - target[index][channel]) ** 2
                              for channel in range(3)))
               for color in set(colors)}
    identity = [card.callsign.upper(), card.card_id, card.width, card.height, len(target)]
    prefix = json.dumps(identity, separators=(",", ":")).encode("utf-8")
    return hashlib.blake2s(prefix + bytes(mapping[color] for color in colors),
                           digest_size=8).hexdigest()


def _as_card(data: object, *, title: str, path: Path | None = None,
             category: str = "") -> PixelCard:
    if not isinstance(data, dict):
        raise ValueError("PixelQSO card must be a JSON object")
    width, height = data.get("width", 32), data.get("height", 32)
    if (type(width) is not int or type(height) is not int or
            not 1 <= width <= _MAX_DIMENSION or not 1 <= height <= _MAX_DIMENSION):
        raise ValueError("KISSChat supports PixelQSO cards up to 64×64 pixels")
    raw_palette = data.get("palette")
    if not isinstance(raw_palette, list) or len(raw_palette) not in _PALETTE_SIZES:
        raise ValueError("PixelQSO palette must contain 2, 8, 16, or 32 colors")
    palette = []
    for color in raw_palette:
        if (not isinstance(color, (list, tuple)) or len(color) != 3 or
                any(type(value) is not int or not 0 <= value <= 15 for value in color)):
            raise ValueError("PixelQSO colors must be RGB444 triples")
        palette.append(tuple(color))
    raw_pixels = data.get("pixels")
    if (not isinstance(raw_pixels, list) or len(raw_pixels) != width * height or
            any(type(value) is not int or not 0 <= value < len(palette)
                for value in raw_pixels)):
        raise ValueError("PixelQSO raster has an invalid size or palette index")
    callsign, grid = data.get("callsign", ""), data.get("grid", "")
    if not isinstance(callsign, str) or not isinstance(grid, str):
        raise ValueError("PixelQSO callsign and grid must be text")
    callsign, grid = callsign.upper(), grid.upper()
    try:
        call_bytes, grid_bytes = callsign.encode("ascii"), grid.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("PixelQSO callsign and grid must be ASCII") from exc
    if (len(call_bytes) > 12 or len(grid_bytes) > 8 or
            not all(char.isprintable() for char in callsign + grid)):
        raise ValueError("PixelQSO callsign/grid exceed card format limits")
    card_id = data.get("card_id", 0xC0DE)
    if type(card_id) is not int or not 0 <= card_id <= 0xFFFF:
        raise ValueError("PixelQSO card ID must be a 16-bit integer")
    animation_link = _validate_animation_link(data.get("animation"), card_id)
    title = "".join(char for char in str(title).strip() if char.isprintable())
    title = title or "PixelQSO card"
    title_bytes = title.encode("utf-8")[:_MAX_TITLE_BYTES]
    while title_bytes:
        try:
            title = title_bytes.decode("utf-8")
            break
        except UnicodeDecodeError:
            title_bytes = title_bytes[:-1]
    title = title or "PixelQSO card"
    return PixelCard(title, callsign, grid, card_id, width, height,
                     tuple(palette), tuple(raw_pixels), path, category,
                     animation_link is not None, animation_link)


def load_pixel_card(path: str | Path, *, category: str = "") -> PixelCard:
    """Load and validate the static raster from a PixelQSO JSON card."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > _MAX_CARD_FILE_BYTES:
        raise ValueError("PixelQSO card file is larger than 2 MiB")
    try:
        data = json.loads(source.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("PixelQSO card is not valid UTF-8 JSON") from exc
    return _as_card(data, title=source.stem, path=source, category=category)


def _pack_pixels(pixels: tuple[int, ...], palette_size: int) -> bytes:
    bits_per_pixel = (palette_size - 1).bit_length()
    output = bytearray()
    accumulator = bit_count = 0
    for pixel in pixels:
        accumulator = (accumulator << bits_per_pixel) | pixel
        bit_count += bits_per_pixel
        while bit_count >= 8:
            bit_count -= 8
            output.append((accumulator >> bit_count) & 0xFF)
            accumulator &= (1 << bit_count) - 1
    if bit_count:
        output.append(accumulator << (8 - bit_count))
    return bytes(output)


def _unpack_pixels(data: bytes, count: int, palette_size: int) -> tuple[int, ...]:
    bits_per_pixel = (palette_size - 1).bit_length()
    values = []
    accumulator = bit_count = offset = 0
    for _ in range(count):
        while bit_count < bits_per_pixel:
            accumulator = (accumulator << 8) | data[offset]
            offset += 1
            bit_count += 8
        bit_count -= bits_per_pixel
        values.append((accumulator >> bit_count) & ((1 << bits_per_pixel) - 1))
        accumulator &= (1 << bit_count) - 1
    return tuple(values)


def _encode_frame_payload(card: PixelCard) -> bytes:
    call_bytes = card.callsign.encode("ascii")
    grid_bytes = card.grid.encode("ascii")
    title_bytes = card.title.encode("utf-8")
    if (len(title_bytes) > _MAX_TITLE_BYTES or len(call_bytes) > 12 or
            len(grid_bytes) > 8 or len(card.palette) not in _PALETTE_SIZES or
            not 1 <= card.width <= _MAX_DIMENSION or
            not 1 <= card.height <= _MAX_DIMENSION or
            len(card.pixels) != card.width * card.height or
            type(card.card_id) is not int or not 0 <= card.card_id <= 0xFFFF):
        raise ValueError("PixelQSO card cannot be represented in a KISSChat message")
    if (any(type(value) is not int or not 0 <= value < len(card.palette)
            for value in card.pixels) or
            any(type(value) is not int or not 0 <= value <= 15
                for color in card.palette for value in color)):
        raise ValueError("PixelQSO card contains an invalid palette index")
    palette_bytes = bytes(value for color in card.palette for value in color)
    body = (call_bytes + grid_bytes + title_bytes + palette_bytes +
            _pack_pixels(card.pixels, len(card.palette)))
    header = _HEADER.pack(_CARD_MAGIC, card.width, card.height, len(card.palette),
                          card.card_id, len(call_bytes), len(grid_bytes),
                          len(title_bytes), 0)
    checksum = zlib.crc32(header[:-4] + body)
    payload = header[:-4] + struct.pack(">I", checksum) + body
    return payload


def _decode_frame_payload(payload: bytes) -> PixelCard:
    if len(payload) < _HEADER.size:
        raise ValueError("PixelQSO card message is truncated")
    (magic, width, height, palette_size, card_id, call_len, grid_len,
     title_len, checksum) = _HEADER.unpack_from(payload)
    if (magic != _CARD_MAGIC or not 1 <= width <= _MAX_DIMENSION or
            not 1 <= height <= _MAX_DIMENSION or palette_size not in _PALETTE_SIZES or
            call_len > 12 or grid_len > 8 or title_len > _MAX_TITLE_BYTES):
        raise ValueError("PixelQSO card message has an unsupported header")
    bits_per_pixel = (palette_size - 1).bit_length()
    raster_size = (width * height * bits_per_pixel + 7) // 8
    content = payload[_HEADER.size:]
    expected_size = call_len + grid_len + title_len + palette_size * 3 + raster_size
    if (len(content) != expected_size or
            zlib.crc32(payload[:_HEADER.size - 4] + content) != checksum):
        raise ValueError("PixelQSO card message failed its size or CRC check")
    offset = 0
    try:
        callsign = content[offset:offset + call_len].decode("ascii")
        offset += call_len
        grid = content[offset:offset + grid_len].decode("ascii")
        offset += grid_len
        title = content[offset:offset + title_len].decode("utf-8")
        offset += title_len
    except UnicodeDecodeError as exc:
        raise ValueError("PixelQSO card identity is not valid text") from exc
    if (not all(char.isprintable() for char in callsign + grid) or
            not all(char.isprintable() for char in title)):
        raise ValueError("PixelQSO card identity contains non-printable text")
    callsign, grid = callsign.upper(), grid.upper()
    palette = tuple(tuple(content[offset + index * 3:offset + index * 3 + 3])
                    for index in range(palette_size))
    offset += palette_size * 3
    if any(value > 15 for color in palette for value in color):
        raise ValueError("PixelQSO card palette is outside RGB444")
    pixels = _unpack_pixels(content[offset:], width * height, palette_size)
    if any(value >= palette_size for value in pixels):
        raise ValueError("PixelQSO card contains an invalid palette index")
    return PixelCard(title or "PixelQSO card", callsign, grid, card_id, width,
                     height, palette, pixels)


def _encode_animation_payload(animation: PixelCardAnimation) -> bytes:
    first, second = animation.frames
    reference = second.animation_link or {}
    expected_link = {"first_card_id": first.card_id,
                     "first_digest": animation.first_digest,
                     "frame_ms": animation.frame_ms}
    if (type(animation.frame_ms) is not int or not 100 <= animation.frame_ms <= 10000 or
            reference != expected_link or first.animation_link is not None or
            first.card_id == second.card_id or
            first.callsign.casefold() != second.callsign.casefold() or
            (first.width, first.height) != (second.width, second.height) or
            pixelqso_artwork_digest(first) != animation.first_digest):
        raise ValueError("PixelQSO animation frames do not match their saved link")
    first_payload = _encode_frame_payload(first)
    second_payload = _encode_frame_payload(second)
    header = _ANIMATION_HEADER.pack(
        _ANIMATION_MAGIC, animation.frame_ms, first.card_id,
        bytes.fromhex(animation.first_digest), len(first_payload),
        len(second_payload), 0)
    body = first_payload + second_payload
    checksum = zlib.crc32(header[:-4] + body)
    return header[:-4] + struct.pack(">I", checksum) + body


def encode_pixel_card_message(card: PixelCard | PixelCardAnimation) -> str:
    """Encode a card or linked animation into its separate chat-message type."""
    if isinstance(card, PixelCardAnimation):
        payload, prefix = _encode_animation_payload(card), ANIMATION_MESSAGE_PREFIX
    elif isinstance(card, PixelCard):
        payload, prefix = _encode_frame_payload(card), CARD_MESSAGE_PREFIX
    else:
        raise TypeError("expected a PixelQSO card or linked animation")
    message = prefix + base64.urlsafe_b64encode(payload).decode("ascii")
    if len(message.encode("utf-8")) > MAX_MESSAGE_BYTES:
        raise ValueError("PixelQSO card or animation is too large for one KISSChat message")
    return message


def _decode_animation_payload(payload: bytes) -> PixelCardAnimation:
    if len(payload) < _ANIMATION_HEADER.size:
        raise ValueError("PixelQSO animation message is truncated")
    (magic, frame_ms, first_id, first_digest_bytes, first_size, second_size,
     checksum) = _ANIMATION_HEADER.unpack_from(payload)
    body = payload[_ANIMATION_HEADER.size:]
    if (magic != _ANIMATION_MAGIC or not 100 <= frame_ms <= 10000 or
            not first_size or not second_size or len(body) != first_size + second_size or
            zlib.crc32(payload[:_ANIMATION_HEADER.size - 4] + body) != checksum):
        raise ValueError("PixelQSO animation failed its size or CRC check")
    first = _decode_frame_payload(body[:first_size])
    second = _decode_frame_payload(body[first_size:])
    first_digest = first_digest_bytes.hex()
    if (first.card_id != first_id or first.card_id == second.card_id or
            first.animation_link is not None or
            first.callsign.casefold() != second.callsign.casefold() or
            (first.width, first.height) != (second.width, second.height) or
            pixelqso_artwork_digest(first) != first_digest):
        raise ValueError("PixelQSO animation frames do not match their saved link")
    link = {"first_card_id": first_id, "first_digest": first_digest,
            "frame_ms": frame_ms}
    second = replace(second, animated=True, animation_link=link)
    return PixelCardAnimation(second.title, first, second, frame_ms,
                              first_digest, second.path, second.category)


def encode_pixel_card_payload(card: PixelCard) -> bytes:
    """Encode one unique card's raster for on-demand attachment transfer."""
    if not isinstance(card, PixelCard):
        raise TypeError("a card attachment contains one PixelQSO card")
    payload = _encode_frame_payload(card)
    if len(payload) > MAX_CACHED_THUMBNAIL_BYTES:
        raise ValueError("PixelQSO card payload exceeds the attachment limit")
    return payload


def decode_pixel_card_payload(payload: bytes) -> PixelCard:
    """Decode one opaque card payload after its transport digest passes."""
    if not isinstance(payload, bytes):
        raise ValueError("PixelQSO card payload must be bytes")
    if payload.startswith(_CARD_MAGIC):
        return _decode_frame_payload(payload)
    raise ValueError("PixelQSO card payload has an unknown format")


def _summary_dict(card: PixelCard, *, include_link: bool) -> dict:
    return {"title": card.title, "callsign": card.callsign, "grid": card.grid,
            "card_id": card.card_id, "width": card.width, "height": card.height,
            "palette": [list(color) for color in card.palette],
            "animation": dict(card.animation_link) if include_link and card.animation_link else None}


def _summary_from_dict(value: object) -> PixelCardSummary:
    if not isinstance(value, dict):
        raise ValueError("PixelQSO card reference frame must be an object")
    title = value.get("title")
    callsign, grid = value.get("callsign"), value.get("grid")
    card_id, width, height = value.get("card_id"), value.get("width"), value.get("height")
    palette_data = value.get("palette")
    try:
        title_length = len(title.encode("utf-8")) if isinstance(title, str) else 99
    except UnicodeEncodeError as exc:
        raise ValueError("PixelQSO card reference title is invalid UTF-8") from exc
    try:
        call_length = len(callsign.encode("ascii")) if isinstance(callsign, str) else 99
        grid_length = len(grid.encode("ascii")) if isinstance(grid, str) else 99
    except UnicodeEncodeError:
        call_length = grid_length = 99
    if (not isinstance(title, str) or not title or title_length > _MAX_TITLE_BYTES or
            not all(char.isprintable() for char in title) or
            not isinstance(callsign, str) or not isinstance(grid, str) or
            not all(char.isprintable() for char in callsign + grid) or
            call_length > 12 or grid_length > 8 or
            type(card_id) is not int or not 0 <= card_id <= 0xFFFF or
            type(width) is not int or not 1 <= width <= _MAX_DIMENSION or
            type(height) is not int or not 1 <= height <= _MAX_DIMENSION or
            not isinstance(palette_data, list) or len(palette_data) not in _PALETTE_SIZES):
        raise ValueError("PixelQSO card reference has invalid frame metadata")
    palette = []
    for color in palette_data:
        if (not isinstance(color, list) or len(color) != 3 or
                any(type(component) is not int or not 0 <= component <= 15
                    for component in color)):
            raise ValueError("PixelQSO card reference palette must use RGB444 triples")
        palette.append(tuple(color))
    animation_link = _validate_animation_link(value.get("animation"), card_id)
    return PixelCardSummary(title, callsign.upper(), grid.upper(), card_id, width,
                            height, tuple(palette), animation_link)


def encode_pixel_card_reference(
        card: PixelCard) -> tuple[str, bytes, PixelCardReference]:
    """Build one card's small log descriptor and separately transferable blob."""
    if not isinstance(card, PixelCard):
        raise TypeError("encode each linked PixelQSO card as its own message")
    payload = encode_pixel_card_payload(card)
    frames = (card,)
    summaries = (PixelCardSummary(card.title, card.callsign, card.grid, card.card_id,
                                  card.width, card.height, card.palette,
                                  card.animation_link),)
    reference = PixelCardReference(
        hashlib.sha256(payload).hexdigest(), len(payload), summaries,
        card.animation_link.get("frame_ms") if card.animation_link else None)
    metadata = {"v": 1, "digest": reference.digest, "size": reference.size,
                "frame_ms": reference.frame_ms,
                "frames": [_summary_dict(frame, include_link=True) for frame in frames]}
    descriptor = json.dumps(metadata, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    message = CARD_REFERENCE_PREFIX + base64.urlsafe_b64encode(descriptor).decode("ascii")
    if len(message.encode("utf-8")) > MAX_MESSAGE_BYTES:
        raise ValueError("PixelQSO card reference is too large for one KISSChat message")
    return message, payload, reference


def decode_pixel_card_reference(body: str) -> PixelCardReference | None:
    """Decode the small log descriptor for an on-demand PixelQSO attachment."""
    if not isinstance(body, str) or not body.startswith(CARD_REFERENCE_PREFIX):
        return None
    encoded = body[len(CARD_REFERENCE_PREFIX):]
    if not encoded or len(body.encode("utf-8")) > MAX_MESSAGE_BYTES:
        raise ValueError("PixelQSO card reference has an invalid size")
    try:
        metadata = json.loads(base64.b64decode(encoded, altchars=b"-_", validate=True))
    except (ValueError, binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("PixelQSO card reference is not valid base64 JSON") from exc
    if not isinstance(metadata, dict):
        raise ValueError("PixelQSO card reference metadata is invalid")
    digest, size, frame_ms, raw_frames = (metadata.get("digest"), metadata.get("size"),
                                         metadata.get("frame_ms"), metadata.get("frames"))
    if (type(metadata.get("v")) is not int or metadata.get("v") != 1 or
            not isinstance(digest, str) or len(digest) != 64 or
            any(char not in "0123456789abcdef" for char in digest) or
            type(size) is not int or not 0 < size <= MAX_CACHED_THUMBNAIL_BYTES or
            not isinstance(raw_frames, list) or len(raw_frames) != 1):
        raise ValueError("PixelQSO card reference metadata is invalid")
    frames = tuple(_summary_from_dict(frame) for frame in raw_frames)
    link = frames[0].animation_link
    if link is None:
        if frame_ms is not None:
            raise ValueError("unlinked PixelQSO card reference has a frame interval")
    elif (type(frame_ms) is not int or link["frame_ms"] != frame_ms or
          link["first_card_id"] == frames[0].card_id):
        raise ValueError("PixelQSO linked-frame reference is inconsistent")
    return PixelCardReference(digest, size, frames, frame_ms)


def verify_pixel_card_payload(payload: bytes, reference: PixelCardReference
                              ) -> PixelCard:
    """Check downloaded bytes against the log descriptor and decode the raster."""
    if (len(payload) != reference.size or
            hashlib.sha256(payload).hexdigest() != reference.digest):
        raise ValueError("PixelQSO card attachment failed its size or SHA-256 check")
    if len(reference.frames) != 1:
        raise ValueError("PixelQSO attachment reference must identify one card")
    card = decode_pixel_card_payload(payload)
    expected = _summary_dict_from_summary(reference.frames[0])
    actual = _summary_dict(card, include_link=False)
    expected["animation"] = None
    if actual != expected:
        raise ValueError("PixelQSO card payload does not match its log reference")
    if reference.frames[0].animation_link is not None:
        card = replace(card, animated=True,
                       animation_link=dict(reference.frames[0].animation_link))
    return card


def _summary_dict_from_summary(frame: PixelCardSummary) -> dict:
    return {"title": frame.title, "callsign": frame.callsign, "grid": frame.grid,
            "card_id": frame.card_id, "width": frame.width, "height": frame.height,
            "palette": [list(color) for color in frame.palette],
            "animation": dict(frame.animation_link) if frame.animation_link else None}


def decode_pixel_card_message(body: str) -> PixelCard | PixelCardAnimation | None:
    """Decode a static card or linked two-frame animation; return None for text."""
    if not isinstance(body, str):
        return None
    if body.startswith(CARD_MESSAGE_PREFIX):
        prefix, decode = CARD_MESSAGE_PREFIX, _decode_frame_payload
    elif body.startswith(ANIMATION_MESSAGE_PREFIX):
        prefix, decode = ANIMATION_MESSAGE_PREFIX, _decode_animation_payload
    else:
        return None
    encoded = body[len(prefix):]
    if not encoded or len(body.encode("utf-8")) > MAX_MESSAGE_BYTES:
        raise ValueError("PixelQSO card or animation message has an invalid size")
    try:
        payload = base64.b64decode(encoded, altchars=b"-_", validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("PixelQSO card message has invalid base64 data") from exc
    return decode(payload)


def _data_root(data_dir: str | Path | None) -> Path:
    if data_dir is not None:
        return Path(data_dir).expanduser()
    configured = os.environ.get("PIXELQSO_DATA_DIR") or os.environ.get("PIXELQSO_DATA")
    return (Path(configured).expanduser() if configured else
            Path.home() / ".local" / "share" / "PixelQSO")


def _source_root(source_dir: str | Path | None) -> Path | None:
    if source_dir is not None:
        return Path(source_dir).expanduser()
    configured = os.environ.get("PIXELQSO_SOURCE_DIR")
    candidates = ([Path(configured).expanduser()] if configured else [])
    dev_root = Path(__file__).resolve().parents[2]
    candidates.extend((dev_root / "cardmodem" / "pixelqso-source",
                       dev_root / "cardmodem" / "pixelqso_source"))
    for candidate in candidates:
        if (candidate / "examples" / "starter-cards").is_dir():
            return candidate
    return None


def list_pixel_cards(*, data_dir: str | Path | None = None,
                     source_dir: str | Path | None = None
                     ) -> list[PixelCard]:
    """List personal, sent, and bundled PixelQSO JSON cards, skipping invalid files."""
    root = _data_root(data_dir)
    sources = [(root / "cards", "my"), (root / "sent-cards", "sent")]
    source = _source_root(source_dir)
    if source is not None:
        starter = source / "examples" / "starter-cards"
        sources.append((starter if starter.is_dir() else source, "starter"))
    cards: list[PixelCard] = []
    seen = set()
    for folder, category in sources:
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("*.json"), key=lambda item: item.name.casefold()):
            if path in seen or len(cards) >= _MAX_LIBRARY_CARDS:
                continue
            seen.add(path)
            try:
                cards.append(load_pixel_card(path, category=category))
            except (OSError, ValueError, TypeError, KeyError):
                continue
    return cards


def _received_card_report(card: PixelCard, *, order: int, logged_at: str) -> dict:
    pixels = list(card.pixels)
    metadata = {
        "callsign": card.callsign,
        "grid": card.grid,
        "card_id": card.card_id,
        "width": card.width,
        "height": card.height,
        "palette": [list(color) for color in card.palette],
        "message_type": "card",
        "exact": True,
        "pixel_coverage": [True] * len(pixels),
        "verified_pixel_coverage": [True] * len(pixels),
        "color_stage": "Whole card verified · KISSChat envelope CRC32",
        "verification_scope": "KISSChat card-envelope CRC32",
    }
    if card.animation_link is not None:
        metadata["animation"] = dict(card.animation_link)
    return {"card": metadata, "pixels": pixels, "received_copies": 1,
            "receive_attempts": 1, "order": order, "logged_at": logged_at,
            "verification_scope": "KISSChat card-envelope CRC32"}


def save_received_pixel_card_message(
        message: PixelCard | PixelCardAnimation, *, origin: str, seq: int,
        data_dir: str | Path | None = None) -> Path:
    """Save received card frames in PixelQSO's native qso-log received_cards."""
    if (not isinstance(origin, str) or len(origin) != 32 or
            any(char not in "0123456789abcdef" for char in origin) or
            type(seq) is not int or not 1 <= seq <= 0xFFFFFFFF):
        raise ValueError("invalid KISSChat card message identity")
    frames = message.frames if isinstance(message, PixelCardAnimation) else (message,)
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    received = [_received_card_report(card, order=index + 1, logged_at=now)
                for index, card in enumerate(frames)]
    first = frames[0]
    entry = {
        "peer_callsign": first.callsign or "Unknown",
        "peer_grid": first.grid,
        "started_at": now,
        "updated_at": now,
        "sent_cards": [],
        "received_cards": received,
        "status": "Received via SUperChat",
        "source_application": "SUperChat",
        "source_message": {"origin": origin, "seq": seq},
    }
    folder = _data_root(data_dir) / "qso-log"
    destination = folder / f"qso-kisschat-{origin}-{seq:08x}.json"
    if destination.exists():
        try:
            existing = json.loads(destination.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = None
        if existing and existing.get("source_message") == entry["source_message"]:
            old_cards = existing.get("received_cards", [])
            if (len(old_cards) == len(received) and
                    all(old.get("card") == new.get("card") and
                        old.get("pixels") == new.get("pixels")
                        for old, new in zip(old_cards, received))):
                return destination
        raise FileExistsError("PixelQSO received-card log already exists")
    temporary = None
    try:
        folder.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=folder,
                                         prefix=".kisschat-", suffix=".tmp",
                                         delete=False) as output:
            temporary = Path(output.name)
            json.dump(entry, output, indent=2)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


def render_pixel_card(card: PixelCard, *, max_width: int = 36,
                      max_height: int = 18) -> Text:
    """Render a compact nearest-neighbor card preview with terminal true color."""
    if max_width < 1 or max_height < 1:
        raise ValueError("card preview dimensions must be positive")
    width = min(card.width, max_width)
    height = min(card.height, max_height * 2)
    image = [[card.pixels[min(card.height - 1, int((y + 0.5) * card.height / height)) *
                          card.width + min(card.width - 1,
                                           int((x + 0.5) * card.width / width))]
              for x in range(width)] for y in range(height)]
    output = Text()
    for y in range(0, height, 2):
        for x in range(width):
            upper = card.palette[image[y][x]]
            lower = card.palette[image[y + 1][x]] if y + 1 < height else upper
            foreground = "#" + "".join(f"{value * 17:02x}" for value in upper)
            background = "#" + "".join(f"{value * 17:02x}" for value in lower)
            output.append("▀", style=f"{foreground} on {background}")
        if y + 2 < height:
            output.append("\n")
    return output
