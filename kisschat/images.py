"""Small, bounded image thumbnails for low-bandwidth chat links."""
from __future__ import annotations

from io import BytesIO

from PIL import Image, ImageOps
from rich.text import Text

from .protocol import MAX_THUMBNAIL_BYTES


MAX_EDGE = 160
# The inline renderer uses at most 40 columns by 18 terminal rows, with two
# vertical pixels per row. Keep new radio images at twice that pixel size.
SEND_MAX_WIDTH = 80
SEND_MAX_HEIGHT = 72
MAX_INPUT_BYTES = 20 * 1024 * 1024


def make_thumbnail(path: str) -> tuple[bytes, int, int]:
    """Normalize an image to a compact JPEG thumbnail suitable for radio."""
    from pathlib import Path

    source = Path(path).expanduser()
    if source.stat().st_size > MAX_INPUT_BYTES:
        raise ValueError("image file is larger than 20 MiB")
    with Image.open(source) as opened:
        if opened.width * opened.height > 40_000_000:
            raise ValueError("image dimensions exceed 40 megapixels")
        image = ImageOps.exif_transpose(opened)
        image.thumbnail((SEND_MAX_WIDTH, SEND_MAX_HEIGHT), Image.Resampling.LANCZOS)
        if "A" in image.getbands() or "transparency" in image.info:
            rgba = image.convert("RGBA")
            flattened = Image.new("RGB", rgba.size, "white")
            flattened.paste(rgba, mask=rgba.getchannel("A"))
            image = flattened
        else:
            image = image.convert("RGB")
    for width, quality in ((80, 58), (64, 54), (48, 48), (32, 44)):
        candidate = image.copy()
        candidate.thumbnail((width, SEND_MAX_HEIGHT), Image.Resampling.LANCZOS)
        output = BytesIO()
        candidate.save(output, format="JPEG", quality=quality, optimize=True,
                       progressive=False)
        data = output.getvalue()
        if len(data) <= MAX_THUMBNAIL_BYTES:
            return data, candidate.width, candidate.height
    raise ValueError("could not compress image below 8 KiB")


def is_valid_thumbnail(data: bytes) -> bool:
    """Check cached radio media without decoding dimensions outside our limits."""
    try:
        with Image.open(BytesIO(data)) as opened:
            if (opened.format != "JPEG" or not 1 <= opened.width <= MAX_EDGE or
                    not 1 <= opened.height <= MAX_EDGE):
                return False
            opened.verify()
        return True
    except (OSError, ValueError, Image.DecompressionBombError,
            Image.DecompressionBombWarning):
        return False


def render_thumbnail(data: bytes, max_width: int = 40, max_height: int = 18) -> Text:
    """Render an already requested thumbnail inline using terminal half-blocks."""
    try:
        with Image.open(BytesIO(data)) as opened:
            if (opened.format != "JPEG" or not 1 <= opened.width <= MAX_EDGE or
                    not 1 <= opened.height <= MAX_EDGE):
                raise ValueError("invalid radio thumbnail")
            image = opened.convert("RGB")
            image.thumbnail((max_width, max_height * 2), Image.Resampling.LANCZOS)
            pixels = image.load()
            rendered = Text()
            for y in range(0, image.height, 2):
                for x in range(image.width):
                    top = pixels[x, y]
                    bottom = pixels[x, y + 1] if y + 1 < image.height else (0, 0, 0)
                    rendered.append("▀", style=(f"#{top[0]:02x}{top[1]:02x}{top[2]:02x} "
                                                  f"on #{bottom[0]:02x}{bottom[1]:02x}{bottom[2]:02x}"))
                rendered.append("\n")
            return rendered
    except (OSError, ValueError, Image.DecompressionBombError,
            Image.DecompressionBombWarning):
        return Text("[thumbnail unavailable]", style="#8a98ae")
