"""Mention parsing and formatting shared by the chat and terminal UI."""
from __future__ import annotations

import re


MENTION = re.compile(r'(?<![\w])@(?:"([^"\n]{1,16})"|([A-Za-z0-9_/-]{1,16}))')
MENTION_RE = re.compile(
    r'(?<![\w])@"[^"\n]{1,16}"|(?<![\w])@[A-Za-z0-9_/-]{1,16}')
BARE_MENTION = re.compile(r"^[A-Za-z0-9_/-]{1,16}$")


def mention_name(match: re.Match[str]) -> str:
    """Return the alias without its @ prefix or optional quotation marks."""
    quoted, bare = match.groups()
    return quoted if quoted is not None else bare


def format_mention(name: str) -> str:
    """Format aliases with spaces in the unambiguous quoted mention syntax."""
    if BARE_MENTION.fullmatch(name):
        return f"@{name}"
    return f'@"{name}"'


def valid_mention_name(name: object) -> bool:
    """Validate an alias that can be represented in a receipt record."""
    return (isinstance(name, str) and bool(name) and '"' not in name and
            len(name.encode("utf-8")) <= 16 and all(char.isprintable() for char in name))
