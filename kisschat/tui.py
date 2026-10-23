"""Pane-based terminal interface for SUperChat."""
from __future__ import annotations

import asyncio
import re
import queue
import shlex
import shutil
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

from rich.text import Text
from rich.console import Group
from rich.padding import Padding
from rich.markup import escape
from rich.style import Style
from rich.syntax import Syntax
from pygments.lexers import get_lexer_by_name
from pygments.util import ClassNotFound
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual import events
from textual.css.query import NoMatches
from textual.screen import ModalScreen
from textual.suggester import Suggester
from textual.widgets import (Button, Checkbox, Footer, Input, Label, OptionList,
                             RichLog, Select, Static)
from textual.widgets.option_list import Option

from .app import Chat
from .images import render_thumbnail
from .pixel_cards import (ANIMATION_MESSAGE_PREFIX, CARD_MESSAGE_PREFIX,
                         CARD_REFERENCE_PREFIX, PixelCard, PixelCardAnimation,
                         PixelCardReference, decode_pixel_card_message,
                         decode_pixel_card_reference, list_pixel_cards,
                         pixelqso_artwork_digest, render_pixel_card,
                         verify_pixel_card_payload)
from .mentions import (BARE_MENTION, MENTION, MENTION_RE, format_mention,
                       mention_name, valid_mention_name)


IMAGE_SUFFIXES = {".bmp", ".gif", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"}
MESSAGE_HIGHLIGHT_SECONDS = 12


def _image_path_from_paste(text: str) -> str | None:
    """Resolve one terminal-dropped image path or local URI-list entry."""
    # Some terminals paste a plain path, while file managers may expose a
    # text/uri-list containing comments and CRLF separators. Accept one entry
    # from either representation, but never guess which file to attach when
    # several are dropped together.
    lines = [line.strip() for line in text.splitlines()]
    entries = [line for line in lines if line and not line.startswith("#")]
    if len(entries) != 1:
        return None

    def resolve(value: str) -> str | None:
        if value.casefold().startswith("file:"):
            uri = urlparse(value)
            if uri.scheme != "file" or uri.netloc not in ("", "localhost"):
                return None
            value = unquote(uri.path)
        path = Path(value).expanduser()
        if path.suffix.casefold() not in IMAGE_SUFFIXES or not path.is_file():
            return None
        return str(path.resolve())

    # Several terminals paste a dropped filename with spaces but without shell
    # quoting. Try the complete line first so it remains a single path.
    entry = entries[0]
    path = resolve(entry)
    if path:
        return path
    # Other terminals quote or escape dropped paths; accept those as well.
    try:
        fields = shlex.split(entry)
    except ValueError:
        return None
    return resolve(fields[0]) if len(fields) == 1 else None


class ComposerInput(Input):
    """Recognize terminal file drops while preserving normal pasted text."""

    def __init__(self, *, on_image_drop, **kwargs):
        super().__init__(**kwargs)
        self.on_image_drop = on_image_drop

    def _on_paste(self, event: events.Paste) -> None:
        path = _image_path_from_paste(event.text)
        if path and self.on_image_drop(path):
            event.stop()
            event.prevent_default()
            return
        super()._on_paste(event)


class ImagePathInput(Input):
    """Normalize terminal-dropped paths inside the image attachment dialog."""

    def _on_paste(self, event: events.Paste) -> None:
        path = _image_path_from_paste(event.text)
        if path:
            self.value = path
            self.cursor_position = len(path)
            event.stop()
            event.prevent_default()
            return
        super()._on_paste(event)


class ChatSuggester(Suggester):
    """Complete mentions and connected-mode calls from heard stations."""

    def __init__(self, names, stations):
        super().__init__(use_cache=False, case_sensitive=False)
        self.names, self.stations = names, stations

    async def get_suggestion(self, value: str) -> str | None:
        connect = re.search(r"(?i)(?<!\S)/connect\s+([A-Za-z0-9/-]*)$", value)
        if connect:
            prefix = connect.group(1).casefold()
            for callsign in self.stations():
                if callsign.casefold().startswith(prefix) and callsign.casefold() != prefix:
                    return value[:connect.start(1)] + callsign + " "
        match = re.search(r'(?<![\w])@"([^"\n]*)$', value)
        if match:
            prefix = match.group(1).casefold()
            for name in self.names():
                if valid_mention_name(name) and name.casefold().startswith(prefix):
                    return value[:match.start(1)] + name + '" '
            return None
        match = re.search(r"(?<![\w])@([A-Za-z0-9_/-]*)$", value)
        if not match:
            return None
        prefix = match.group(1).casefold()
        for name in self.names():
            if BARE_MENTION.fullmatch(name) and name.casefold().startswith(prefix):
                return value[:match.start(1)] + name + " "
        return None


class ChannelSwitcher(ModalScreen[str | None]):
    BINDINGS = [("escape", "dismiss(None)", "Close")]
    CSS = """
    ChannelSwitcher { align: center middle; background: $background 75%; }
    #switcher-box { width: 1fr; max-width: 62; height: 18; margin: 0 1; padding: 1 2; border: round $accent; background: #111827; }
    #switcher-title { height: 1; color: #d8ccff; text-style: bold; margin-bottom: 1; }
    #channel-search { margin-bottom: 1; }
    #channel-matches { height: 1fr; border: none; background: #111827; }
    """

    def __init__(self, channels: list[str]):
        super().__init__()
        self.channels = channels
        self.matches = channels

    def compose(self) -> ComposeResult:
        with Vertical(id="switcher-box"):
            yield Label("JUMP TO CHANNEL", id="switcher-title")
            yield Input(placeholder="Type a channel name…", id="channel-search")
            yield OptionList(id="channel-matches")

    def on_mount(self) -> None:
        self._fill("")
        self.query_one("#channel-search", Input).focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "channel-search":
            self._fill(event.value)

    def _fill(self, query: str) -> None:
        needle = query.casefold().lstrip("#")
        if not needle:
            self.matches = list(self.channels)
        else:
            def rank(name: str):
                value = name.lstrip("#").casefold()
                if needle in value:
                    return (0, value.index(needle), len(value))
                pos = -1
                for char in needle:
                    pos = value.find(char, pos + 1)
                    if pos < 0:
                        return None
                return (1, pos, len(value))
            ranked = [(score, name) for name in self.channels
                      if (score := rank(name)) is not None]
            self.matches = [name for _, name in sorted(ranked)]
        options = [Option(f"  {name}", id=f"match-{index}")
                   for index, name in enumerate(self.matches)]
        listing = self.query_one("#channel-matches", OptionList)
        listing.clear_options()
        listing.add_options(options)
        if options:
            listing.highlighted = 0

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id == "channel-matches" and event.option_index < len(self.matches):
            self.dismiss(self.matches[event.option_index])

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "channel-search":
            query = event.value.strip()
            if query.startswith("#") and re.fullmatch(r"#[A-Za-z0-9_-]{1,31}", query):
                self.dismiss(query)
            elif self.matches:
                self.dismiss(self.matches[0])


class MessageSearchScreen(ModalScreen[str | None]):
    BINDINGS = [("escape", "dismiss(None)", "Close")]
    CSS = """
    MessageSearchScreen { align: center middle; background: $background 75%; }
    #message-search-box { width: 1fr; max-width: 72; height: 13; margin: 0 1; padding: 1 2; border: round $accent; background: #111827; }
    #message-search-title { height: 1; color: #d8ccff; text-style: bold; margin-bottom: 1; }
    #message-search-help { height: 2; color: #8a98ae; margin-top: 1; }
    #message-search-actions { height: 3; align: right middle; }
    #message-search-actions Button { margin-left: 1; }
    """

    def __init__(self, query: str = ""):
        super().__init__()
        self.query = query

    def compose(self) -> ComposeResult:
        with Vertical(id="message-search-box"):
            yield Label("SEARCH THIS CHANNEL", id="message-search-title")
            yield Input(value=self.query, placeholder="Find text in local and synced history…",
                        id="message-search-input")
            yield Static("Search is literal and case-insensitive. Leave empty to show the timeline.",
                         id="message-search-help")
            with Horizontal(id="message-search-actions"):
                yield Button("Cancel", id="message-search-cancel")
                yield Button("Search", id="message-search-submit", variant="primary")

    def on_mount(self) -> None:
        field = self.query_one("#message-search-input", Input)
        field.focus()
        field.cursor_position = len(field.value)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "message-search-input":
            self.dismiss(event.value)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "message-search-cancel":
            self.dismiss(None)
        elif event.button.id == "message-search-submit":
            self.dismiss(self.query_one("#message-search-input", Input).value)


class ImageAttachScreen(ModalScreen[tuple[str, str] | None]):
    BINDINGS = [("escape", "dismiss(None)", "Close")]
    CSS = """
    ImageAttachScreen { align: center middle; background: $background 75%; }
    #image-box { width: 1fr; max-width: 76; height: 19; margin: 0 1; padding: 1 2; border: round $accent; background: #111827; }
    #image-actions { width: 100%; height: 3; }
    #image-actions Button {
        width: 1fr;
        min-width: 12;
        height: 3;
        margin-right: 1;
        padding: 0 1;
        color: #f0edff;
        background: #26334a;
        border: round #596985;
        text-style: bold;
        content-align: center middle;
    }
    #image-actions #send-image {
        margin-right: 0;
        color: #ffffff;
        background: #493d78;
        border: round #8f7bd4;
    }
    #image-title { height: 1; color: #d8ccff; text-style: bold; margin-bottom: 1; }
    #image-caption { margin-top: 1; }
    #image-help { height: 2; color: #aebbd0; margin-top: 1; }
    """

    def __init__(self, path: str = ""):
        super().__init__()
        self.path = path

    def compose(self) -> ComposeResult:
        with Vertical(id="image-box"):
            yield Label("ATTACH A RADIO THUMBNAIL", id="image-title")
            yield ImagePathInput(value=self.path, placeholder="Path to JPEG, PNG, WebP…",
                                 id="image-path")
            yield Input(placeholder="Caption (optional)", id="image-caption")
            yield Static("Enter sends · Tab adds a caption · Thumbnail ≤160 px / 8 KiB\n"
                         "Image bytes are sent only when requested.", id="image-help")
            with Horizontal(id="image-actions"):
                yield Button("Browse files", id="browse-image", variant="default")
                yield Button("Send image", id="send-image", variant="primary")

    def on_mount(self) -> None:
        self.query_one("#image-path", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "browse-image":
            self.run_worker(self._browse_image(), exclusive=True, group="image-picker")
        elif event.button.id == "send-image":
            self._submit_image()

    async def _browse_image(self) -> None:
        """Use an optional desktop chooser without blocking Textual's event loop."""
        zenity = shutil.which("zenity")
        kdialog = shutil.which("kdialog")
        if zenity:
            command = [zenity, "--file-selection", "--title=Choose an image",
                       "--file-filter=Image files | *.png *.jpg *.jpeg *.webp *.gif *.bmp *.tif *.tiff"]
        elif kdialog:
            command = [kdialog, "--getopenfilename", str(Path.home()),
                       "Image files (*.png *.jpg *.jpeg *.webp *.gif *.bmp *.tif *.tiff)"]
        else:
            self.query_one("#image-help", Static).update(
                "No desktop file chooser found. Paste an image path above.")
            return
        try:
            process = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await process.communicate()
        except OSError as exc:
            self.query_one("#image-help", Static).update(f"Could not open file chooser: {exc}")
            return
        if process.returncode == 0:
            path = stdout.decode("utf-8", "replace").strip()
            if path:
                field = self.query_one("#image-path", Input)
                field.value = path
                self.query_one("#image-caption", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "image-path":
            self._submit_image()
        elif event.input.id == "image-caption":
            self._submit_image()

    def _submit_image(self) -> None:
        path = self.query_one("#image-path", Input).value.strip()
        caption = self.query_one("#image-caption", Input).value.strip()
        if path:
            self.dismiss((path, caption))
        else:
            self.query_one("#image-path", Input).focus()


class ClearHistoryScreen(ModalScreen[bool]):
    BINDINGS = [("escape", "dismiss(False)", "Cancel")]
    CSS = """
    ClearHistoryScreen { align: center middle; background: $background 75%; }
    #clear-history-box { width: 1fr; max-width: 72; height: auto; margin: 1 2; padding: 1 2; border: round $error; background: #111827; }
    #clear-history-title { color: #ffd0d0; text-style: bold; margin-bottom: 1; }
    #clear-history-help { height: auto; margin-bottom: 1; color: #c4ccda; }
    #clear-history-actions { width: 100%; height: 3; }
    #clear-history-actions Button { width: 1fr; margin-right: 1; }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="clear-history-box"):
            yield Label("CLEAR LOCAL MESSAGE HISTORY?", id="clear-history-title")
            yield Static("This deletes all messages and cached image data on this device. "
                         "Queued frames are discarded; a frame already transmitting may finish. "
                         "Other stations' copies cannot be erased and may sync back later.",
                         id="clear-history-help")
            with Horizontal(id="clear-history-actions"):
                yield Button("Cancel", id="cancel-clear", variant="default")
                yield Button("Delete local history", id="confirm-clear", variant="error")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm-clear")


class ImagePicker(ModalScreen[dict | None]):
    BINDINGS = [("escape", "dismiss(None)", "Close")]
    CSS = """
    ImagePicker { align: center middle; background: $background 75%; }
    #image-picker-box { width: 1fr; max-width: 72; height: 20; margin: 0 1; padding: 1 2; border: round $accent; background: #111827; }
    #image-picker-title { height: 1; color: #d8ccff; text-style: bold; margin-bottom: 1; }
    #image-picker-list { height: 1fr; border: none; background: #111827; }
    #image-picker-empty { height: 1fr; color: #8a98ae; content-align: center middle; }
    #image-picker-hint { height: 1; color: #8a98ae; margin-top: 1; }
    """

    def __init__(self, images: list[dict], store):
        super().__init__()
        self.images, self.store = images, store

    def compose(self) -> ComposeResult:
        with Vertical(id="image-picker-box"):
            yield Label("REQUEST A CHANNEL THUMBNAIL", id="image-picker-title")
            if self.images:
                options = []
                for index, image in enumerate(self.images):
                    stamp = time.strftime("%H:%M", time.localtime(int(image["ts"])))
                    if self.store.image_for(image["digest"]):
                        state = "Downloaded"
                    else:
                        received, total = self.store.image_progress(image["origin"],
                                                                    int(image["seq"]))
                        state = (f"Resume {received}/{total}" if received
                                 else "Select to fetch")
                    options.append(Option(f"{escape(str(image['nick']))} · {stamp} · "
                                          f"{image['width']}×{image['height']} · {state}",
                                          id=f"image-{index}"))
                yield OptionList(*options, id="image-picker-list")
                yield Static("Select an image to request its thumbnail", id="image-picker-hint")
            else:
                yield Static("No image attachments in this channel yet.", id="image-picker-empty")

    def on_mount(self) -> None:
        if self.images:
            self.query_one("#image-picker-list", OptionList).focus()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id == "image-picker-list" and event.option_index < len(self.images):
            self.dismiss(self.images[event.option_index])


class PixelCardPicker(ModalScreen[PixelCard | None]):
    BINDINGS = [("escape", "dismiss(None)", "Close")]
    CSS = """
    PixelCardPicker { align: center middle; background: $background 75%; }
    #pixel-card-box { width: 1fr; max-width: 96; height: 1fr; max-height: 28; margin: 0 1; padding: 1 2; border: round $accent; background: #111827; }
    #pixel-card-title { height: 1; color: #d8ccff; text-style: bold; margin-bottom: 1; }
    #pixel-card-content { height: 1fr; }
    #pixel-card-list-pane { width: 1fr; height: 1fr; }
    #pixel-card-list { height: 1fr; border: none; background: #111827; }
    #pixel-card-help { height: 1; color: #8a98ae; }
    #pixel-card-preview { width: 34; min-width: 28; height: 1fr; margin-left: 1; padding: 1; border: round #374151; background: #0f172a; }
    #pixel-card-content.-stacked { layout: vertical; }
    #pixel-card-content.-stacked #pixel-card-list-pane { width: 1fr; height: 1fr; }
    #pixel-card-content.-stacked #pixel-card-preview { width: 1fr; height: 15; min-height: 13; margin-left: 0; margin-top: 1; }
    #pixel-card-empty { height: 1fr; color: #8a98ae; content-align: center middle; }
    """

    def __init__(self, cards: list[PixelCard]):
        super().__init__()
        self.cards = cards
        self.previewed_index: int | None = None

    def compose(self) -> ComposeResult:
        with Vertical(id="pixel-card-box"):
            yield Label("SEND A PIXELQSO CARD", id="pixel-card-title")
            if self.cards:
                options = []
                labels = {"my": "My card", "sent": "Sent card", "starter": "Starter card"}
                for index, card in enumerate(self.cards):
                    identity = " · ".join(value for value in (card.callsign, card.grid) if value)
                    title = card.title
                    frame = (f" · linked frame 2 → {card.animation_link['first_card_id']:04X} · "
                             f"{card.animation_link['frame_ms']} ms"
                             if card.animation_link else "")
                    label = (f"{labels.get(card.category, 'PixelQSO card')} · {title} · "
                             f"{identity or 'No station'} · {card.width}×{card.height} · "
                             f"{len(card.palette)} colors{frame}")
                    options.append(Option(label, id=f"pixel-card-{index}"))
                with Horizontal(id="pixel-card-content"):
                    with Vertical(id="pixel-card-list-pane"):
                        yield OptionList(*options, id="pixel-card-list")
                        yield Static("↑↓ preview · Enter sends · Esc closes", id="pixel-card-help")
                    yield Static(self._preview_renderable(0), id="pixel-card-preview")
            else:
                yield Static("No PixelQSO cards found. Save a card in PixelQSO or set "
                             "PIXELQSO_DATA_DIR to its data folder.", id="pixel-card-empty")

    def on_mount(self) -> None:
        if self.cards:
            if self.size.width < 106:
                self.query_one("#pixel-card-content", Horizontal).add_class("-stacked")
            listing = self.query_one("#pixel-card-list", OptionList)
            listing.focus()
            if listing.highlighted is None:
                listing.highlighted = 0
            self._update_preview(listing.highlighted or 0)

    def on_option_list_option_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        if event.option_list.id == "pixel-card-list":
            self._update_preview(event.option_index)

    def _update_preview(self, index: int) -> None:
        if 0 <= index < len(self.cards):
            self.previewed_index = index
            self.query_one("#pixel-card-preview", Static).update(
                self._preview_renderable(index))

    def _preview_renderable(self, index: int):
        card = self.cards[index]
        if isinstance(card, PixelCardAnimation):
            title = f"{card.first.title} → {card.second.title}"
            details = (f"Two frames · {card.frame_ms} ms/frame\n"
                       f"{card.callsign} · {card.grid} · {card.width}×{card.height} · "
                       f"{len(card.palette)} colors\nPreview: frame 1")
            image = render_pixel_card(card.first, max_width=24, max_height=8)
        else:
            title = card.title
            details = (f"{card.callsign} · {card.grid} · {card.width}×{card.height} · "
                       f"{len(card.palette)} colors")
            if card.animation_link:
                details += (f"\nFrame 2 · links to {card.animation_link['first_card_id']:04X} · "
                            f"{card.animation_link['frame_ms']} ms/frame")
            image = render_pixel_card(card, max_width=24, max_height=8)
        return Group(Text(title, style="bold #d8ccff"),
                     Text(details, style="#9fb0c7"), image)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id == "pixel-card-list" and event.option_index < len(self.cards):
            card = self.cards[event.option_index]
            self.dismiss(card)


class ChatApp(App[None]):
    TITLE = "SUperChat"
    CSS_PATH = "tui.tcss"
    HORIZONTAL_BREAKPOINTS = [(0, "-compact"), (77, "-medium"), (106, "-wide")]
    VERTICAL_BREAKPOINTS = [(0, "-short"), (40, "-tall")]
    BINDINGS = [
        ("ctrl+shift+k", "switch_channel", "Channels"),
        ("ctrl+shift+f", "search_messages", "Search"),
        ("ctrl+l", "clear_messages", "Clear view"),
        ("ctrl+q", "quit", "Quit"),
    ]

    def __init__(self, chat: Chat):
        super().__init__()
        self.chat = chat
        self.channel = chat.channel
        self.channel_drafts: dict[str, str] = {}
        self.unread: dict[str, int] = {}
        self.unread_mentions: dict[str, int] = {}
        self.attachments: list[dict] = []
        self.history_limit = 200
        self.search_query: str | None = None
        self.displayed_messages: set[tuple[str, int]] = set()
        self.displayed_day: tuple[int, int] | None = None
        self.new_message_times: dict[tuple[str, int], float] = {}
        self.message_line_offsets: dict[tuple[str, int], int] = {}
        self.message_render_cache: dict[tuple[str, int], object] = {}
        self.pixel_card_animations: dict[tuple[str, int], PixelCardAnimation] = {}
        self.pixel_card_frame_indices: dict[tuple[str, int], int] = {}
        self.message_render_search: str | None = None
        self.cleared_messages: set[tuple[str, int]] = set()
        self.last_status = "Broadcast link ready"

    def compose(self) -> ComposeResult:
        with Horizontal(id="shell"):
            with Vertical(id="sidebar", classes="panel"):
                yield Static("✦  SUperChat", id="brand")
                yield Static("RADIO MESH", classes="eyebrow")
                yield Static(f"●  {self.chat.args.callsign}", id="my-station")
                yield Static("CHANNELS", classes="eyebrow section-label")
                yield OptionList(id="channels")
                yield Button("＋  Join channel", id="join-channel", variant="default")
                yield Static("HISTORY", classes="eyebrow section-label")
                yield Static("Synced between stations\nStored on this device", id="history-note")
                yield Static(Text.assemble(
                    ("● You\n", "#c4b5fd"), ("● Direct\n", "#8bd5ca"),
                    ("↪ Relayed\n", "#e7b774"), ("? Path unknown", "#a8b2c3")),
                    id="message-legend")
            with Vertical(id="conversation", classes="panel"):
                with Horizontal(id="room-header"):
                    yield Static(self.channel, id="room-name")
                    yield Static("Shared radio channel  ·  CHAT group", id="room-meta")
                yield Button("↑ Load older", id="load-older", variant="default")
                yield RichLog(id="messages", wrap=True, markup=False,
                              highlight=False, auto_scroll=True)
                yield Static("MESSAGE  ·  Enter to send  ·  @ to mention a station",
                             id="composer-label")
                yield ComposerInput(placeholder=f"Message {self.channel}", id="composer",
                                    suggester=ChatSuggester(self._mention_targets,
                                                            self._connect_targets),
                                    max_length=4096, on_image_drop=self._open_dropped_image)
                with Horizontal(id="composer-actions"):
                    yield Button("＋ Image", id="attach-image", variant="default")
                    yield Button("＋ PixelQSO card", id="attach-card", variant="default")
                yield Static("＋ Image thumbnails · ＋ PixelQSO cards · "
                             "Ctrl+Shift+K channels · /images",
                             id="composer-hint")
            with Vertical(id="rail", classes="panel"):
                yield Static("LINK STATUS", classes="eyebrow")
                yield Static("● CONNECTING", id="link-state")
                detail = self._link_detail()
                yield Static(detail, id="link-detail")
                yield Checkbox("Unattended · auto TX ≤500 Hz",
                               value=self.chat.unattended,
                               disabled=getattr(self.chat.args, "transport", "broadcast") != "broadcast",
                               id="unattended-toggle")
                yield Select([("Auto (widest under limit)", ""),
                              *((f"{name} · {width:g} Hz", name)
                                for name, width in self.chat.data.mode_catalog)],
                             value=(self.chat.data.manual_mode or ""),
                             allow_blank=False,
                             disabled=getattr(self.chat.args, "transport", "broadcast") != "broadcast",
                             id="broadcast-mode")
                yield Select([("No maximum", "0"), ("500 Hz", "500"),
                              ("1200 Hz", "1200"), ("2300 Hz", "2300"),
                              ("2750 Hz", "2750")],
                             value=str(getattr(self.chat.data, "max_bandwidth", None) or 0),
                             allow_blank=False,
                             disabled=getattr(self.chat.args, "transport", "broadcast") != "broadcast",
                             id="broadcast-bandwidth")
                yield Static("YOUR STATION", classes="eyebrow section-label")
                yield Static(f"{self.chat.nick}\n{self.chat.args.callsign}", id="station-card")
                yield Static("HEARD STATIONS", classes="eyebrow section-label")
                yield Static("Listening for stations…", id="station-list")
                yield Static("RECENT ATTACHMENTS", classes="eyebrow section-label")
                yield OptionList(id="image-list")
                yield Button("↓  Request selected", id="request-image", variant="default")
                yield Static("MESH SYNC", classes="eyebrow section-label")
                yield Static("History relays on\nNo central server", id="sync-card")
                yield Static("TRANSMIT", classes="eyebrow section-label")
                yield Static("● IDLE · no frames queued", id="tx-state")
                yield Static("ACTIVITY", classes="eyebrow section-label")
                yield Static(self.last_status, id="activity")
        yield Footer()

    def on_mount(self) -> None:
        unread_channels = self.chat.store.unread_summary()
        for unread in unread_channels:
            channel = unread["c"]
            if channel == self.channel:
                continue
            self.unread[channel] = int(unread["messages"])
            self.unread_mentions[channel] = int(unread["mentions"] or 0)
        if any(unread["c"] == self.channel for unread in unread_channels):
            self.chat.store.clear_unread(self.channel)
        self._refresh_channels()
        self._load_messages()
        self.set_interval(0.15, self._drain_events)
        self.set_interval(0.1, self._animate_pixel_card_messages)
        self.set_interval(1, self._fade_new_messages)
        self.query_one("#composer", Input).focus()
        self._refresh_stations()
        self._refresh_attachments()
        self._refresh_stats()

    def _channels(self) -> list[str]:
        return sorted(set(self.chat.store.channels()) | {self.channel})

    def _refresh_channels(self) -> None:
        listing = self.query_one("#channels", OptionList)
        channels = self._channels()
        listing.clear_options()
        listing.add_options([
            Option(self._channel_label(channel), id=f"channel-{index}")
            for index, channel in enumerate(channels)
        ])
        selected = channels.index(self.channel) if self.channel in channels else 0
        listing.highlighted = selected

    def _channel_label(self, channel: str) -> Text:
        count = self.unread.get(channel, 0)
        active = channel == self.channel
        label = Text("▸ " if active else "  ", style="bold #bda8ff" if active else "#75839a")
        label.append(channel, style="bold #f0edff" if active else "#b7c1d1")
        if count:
            label.append(f"  {count}", style="bold #a7f3d0")
        mentions = self.unread_mentions.get(channel, 0)
        if mentions:
            label.append(f"  @{mentions}", style="bold #f9d56e")
        return label

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id == "channels":
            channels = self._channels()
            if event.option_index < len(channels):
                self._set_channel(channels[event.option_index])

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "join-channel":
            self.action_switch_channel()
        elif event.button.id == "attach-image":
            self.push_screen(ImageAttachScreen(), self._send_attached_image)
        elif event.button.id == "attach-card":
            self.push_screen(PixelCardPicker(list_pixel_cards()), self._send_attached_card)
        elif event.button.id == "request-image":
            listing = self.query_one("#image-list", OptionList)
            if self.attachments and listing.highlighted is not None:
                attachment = self.attachments[listing.highlighted]
                self.chat.command(
                    f"/image-get {attachment['origin']}:{attachment['seq']}")
        elif event.button.id == "load-older":
            self.history_limit += 200
            self._load_messages()

    async def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        if event.checkbox.id != "unattended-toggle":
            return
        checkbox = event.checkbox
        if event.value == self.chat.unattended:
            return
        previous = self.chat.unattended
        checkbox.disabled = True
        try:
            await asyncio.to_thread(self.chat.set_unattended, event.value)
        except Exception as exc:
            checkbox.value = previous
            self.last_status = f"Could not change unattended policy: {exc}"
            self.query_one("#activity", Static).update(self._short_status(self.last_status))
        finally:
            checkbox.disabled = (getattr(self.chat.args, "transport", "broadcast") != "broadcast" or
                                 not self.chat.data.unattended_available)

    async def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "broadcast-mode":
            mode = str(event.value)
            if mode:
                try:
                    await asyncio.to_thread(self.chat.data.set_broadcast_mode, mode)
                    self.chat.args.mode = mode
                    self.last_status = f"Broadcast mode set to {mode}"
                except Exception as exc:
                    self.last_status = f"Could not change broadcast mode: {exc}"
            self.query_one("#activity", Static).update(self._short_status(self.last_status))
        elif event.select.id == "broadcast-bandwidth":
            try:
                value = int(event.value)
                await asyncio.to_thread(self.chat.data.set_max_bandwidth, value or None)
                self.chat.args.max_bandwidth = value or None
                modes = [(name, width) for name, width in self.chat.data.mode_catalog
                         if not value or width <= value]
                mode_select = self.query_one("#broadcast-mode", Select)
                mode_select.set_options([("Auto (widest under limit)", ""),
                                         *((f"{name} · {width:g} Hz", name)
                                           for name, width in modes)])
                mode_select.value = self.chat.data.manual_mode or ""
                self.last_status = (f"Broadcast maximum set to {value} Hz" if value
                                    else "Broadcast maximum removed")
            except Exception as exc:
                self.last_status = f"Could not change bandwidth maximum: {exc}"
                self.query_one("#broadcast-bandwidth", Select).value = str(
                    self.chat.data.max_bandwidth or 0)
            self.query_one("#activity", Static).update(self._short_status(self.last_status))

    def on_input_changed(self, event: Input.Changed) -> None:
        # Some terminal emulators deliver a file drop as ordinary input rather
        # than a bracketed Paste event. Catch and normalize complete local paths
        # in both the composer and the attachment dialog.
        path = _image_path_from_paste(event.value)
        if event.input.id == "image-path":
            if path and event.value != path:
                event.input.value = path
            return
        if event.input.id == "composer" and path:
            event.input.value = ""
            self._open_dropped_image(path)

    def _send_attached_image(self, result: tuple[str, str] | None) -> None:
        if not result:
            return
        path, caption = result
        self.chat.queue_image(path, caption)
        self.query_one("#composer", Input).focus()

    def _send_attached_card(self, card: PixelCard | None) -> None:
        if card:
            self.chat.queue_card(card)
            self.query_one("#composer", Input).focus()

    def _open_dropped_image(self, path: str) -> bool:
        self.push_screen(ImageAttachScreen(path), self._send_attached_image)
        return True

    def _set_channel(self, channel: str) -> None:
        self.chat.command(f"/join {channel}")
        self._show_channel(channel)

    def _show_channel(self, channel: str) -> None:
        composer = self.query_one("#composer", Input)
        self.channel_drafts[self.channel] = composer.value
        self.channel = channel
        composer.value = self.channel_drafts.get(channel, "")
        self.chat.store.clear_unread(channel)
        self.search_query = None
        self.cleared_messages.clear()
        self.history_limit = 200
        self.unread.pop(channel, None)
        self.unread_mentions.pop(channel, None)
        self._apply_channel()

    def _apply_channel(self) -> None:
        self.query_one("#room-name", Static).update(self.channel)
        self.query_one("#room-meta", Static).update(
            "Shared radio channel  ·  CHAT group   ·   local and relayed history")
        self.query_one("#composer", Input).placeholder = f"Message {self.channel}"
        self._refresh_channels()
        self._refresh_attachments()
        self._load_messages()
        self.query_one("#composer", Input).focus()

    def action_switch_channel(self) -> None:
        self.push_screen(ChannelSwitcher(self._channels()), self._switch_result)

    def action_search_messages(self) -> None:
        self.push_screen(MessageSearchScreen(self.search_query or ""), self._search_result)

    def _search_result(self, query: str | None) -> None:
        if query is None:
            self.query_one("#composer", Input).focus()
            return
        self.search_query = query.strip() or None
        self.cleared_messages.clear()
        self.history_limit = 200
        self._load_messages()
        self._refresh_attachments()
        self.query_one("#composer", Input).focus()

    def _switch_result(self, channel: str | None) -> None:
        if channel:
            self._set_channel(channel)

    def _clear_history_result(self, confirmed: bool | None) -> None:
        if confirmed:
            deleted = self.chat.clear_local_history()
            self.search_query = None
            self.history_limit = 200
            self._load_messages()
            self._refresh_attachments()
            self._refresh_channels()
            queued = self.chat.last_purge_queued_frames
            self.last_status = (f"Cleared {deleted} local messages and image data; "
                                f"discarded {queued} queued frames")
            self.query_one("#activity", Static).update(self._short_status(self.last_status))
        self.query_one("#composer", Input).focus()

    def action_clear_messages(self) -> None:
        self.query_one("#messages", RichLog).clear()
        self.cleared_messages.update(self.displayed_messages)
        self.displayed_messages.clear()
        self.new_message_times.clear()
        self.message_line_offsets.clear()
        self.displayed_day = None
        self.last_status = "View cleared · use /history to reload"
        self.query_one("#activity", Static).update(self._short_status(self.last_status))

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "composer":
            return
        content = event.value.strip()
        event.input.value = ""
        if not content:
            return
        if content.casefold() == "/images":
            self.push_screen(ImagePicker(self.chat.store.recent_attachments(
                                             self.channel, kind="image"),
                                         self.chat.store), self._request_picked_image)
            return
        command_parts = content.split(maxsplit=1)
        command = command_parts[0].casefold()
        argument = command_parts[1] if len(command_parts) == 2 else ""
        if command == "/clear-history":
            self.push_screen(ClearHistoryScreen(), self._clear_history_result)
            return
        if command == "/search":
            self._search_result(argument)
            return
        if content.startswith("/"):
            keep_running = self.chat.command(content)
            if not keep_running:
                self.exit()
                return
            if command == "/join" and self.chat.channel != self.channel:
                self._show_channel(self.chat.channel)
            elif command == "/nick":
                self.query_one("#station-card", Static).update(
                    f"{self.chat.nick}\n{self.chat.args.callsign}")
            if command == "/history":
                self.search_query = None
                try:
                    count = max(1, min(200, int(argument.strip() or "30")))
                except ValueError:
                    count = 30
                self.history_limit = count
                self.cleared_messages.clear()
                self._load_messages()
                self._refresh_attachments()
            self._refresh_channels()
            self._refresh_stations()
        else:
            try:
                self.chat.publish(content)
                self._refresh_stats()
            except (OSError, ValueError, OverflowError) as exc:
                event.input.value = content
                event.input.focus()
                self.last_status = f"Message not sent: {exc}"
                self.query_one("#activity", Static).update(self._short_status(self.last_status))

    def _request_picked_image(self, image: dict | None) -> None:
        if image:
            self.chat.command(f"/image-get {image['origin']}:{image['seq']}")
        self.query_one("#composer", Input).focus()

    def _load_messages(self, *, preserve_scroll: bool = False) -> None:
        feed = self.query_one("#messages", RichLog)
        if self.message_render_search != self.search_query:
            self.message_render_cache.clear()
            self.message_render_search = self.search_query
        old_y = feed.scroll_y
        follow = not preserve_scroll or feed.is_vertical_scroll_end
        anchor = None
        if preserve_scroll and not follow:
            for identity, line in self.message_line_offsets.items():
                if line <= old_y:
                    anchor = (identity, old_y - line)
        self.message_line_offsets.clear()
        feed.clear()
        if self.search_query:
            messages = self.chat.store.search_history(self.channel, self.search_query,
                                                     self.history_limit)
        else:
            messages = self.chat.store.history(self.channel, self.history_limit)
        messages = [message for message in messages
                    if (message["o"], int(message["s"])) not in self.cleared_messages]
        self.displayed_messages = {(message["o"], int(message["s"]))
                                   for message in messages}
        self.message_render_cache = {key: value for key, value in self.message_render_cache.items()
                                     if key in self.displayed_messages}
        self.pixel_card_animations = {
            key: value for key, value in self.pixel_card_animations.items()
            if key in self.displayed_messages
        }
        self.pixel_card_frame_indices = {
            key: value for key, value in self.pixel_card_frame_indices.items()
            if key in self.displayed_messages
        }
        self.displayed_day = None
        for message in messages:
            day, marker = self._day_marker(int(message["ts"]))
            if day != self.displayed_day:
                feed.write(marker, scroll_end=False)
                self.displayed_day = day
            self.message_line_offsets[(message["o"], int(message["s"]))] = len(feed.lines)
            feed.write(self._message_renderable(message), scroll_end=False)
        if follow:
            feed.scroll_end(animate=False)
        else:
            if anchor and anchor[0] in self.message_line_offsets:
                old_y = self.message_line_offsets[anchor[0]] + anchor[1]
            feed.scroll_to(y=old_y, animate=False)
        total = (self.chat.store.search_count(self.channel, self.search_query)
                 if self.search_query else self.chat.store.history_count(self.channel))
        meta = self.query_one("#room-meta", Static)
        if self.search_query:
            meta.update(Text(f'Search · "{self.search_query}" · {total} matches',
                             style="#a8b2c3"))
        else:
            meta.update(Text("Shared radio channel  ·  CHAT group   ·  local and relayed history",
                             style="#73839c"))
        loaded = min(self.history_limit, total)
        control = self.query_one("#load-older", Button)
        control.disabled = loaded >= total
        if self.search_query:
            control.label = (f"↑ More matches · {loaded}/{total}" if loaded < total
                             else f"✓ Search results · {total}")
        else:
            control.label = (f"↑ Load older · {loaded}/{total}" if loaded < total
                             else f"✓ All history · {total}")

    def _fade_new_messages(self) -> None:
        if not self.is_mounted or not self.new_message_times:
            return
        now = time.monotonic()
        visible = bool(self.displayed_messages.intersection(self.new_message_times))
        self.new_message_times = {key: stamp for key, stamp in self.new_message_times.items()
                                  if now - stamp < MESSAGE_HIGHLIGHT_SECONDS}
        if visible:
            self._load_messages(preserve_scroll=True)

    def _card_animation_now_ms(self) -> float:
        return time.monotonic() * 1000

    def _animate_pixel_card_messages(self) -> None:
        if not self.is_mounted or not self.pixel_card_animations:
            return
        now_ms = self._card_animation_now_ms()
        changed = False
        for identity, animation in self.pixel_card_animations.items():
            if identity not in self.displayed_messages:
                continue
            frame_index = animation.frame_index(now_ms)
            if self.pixel_card_frame_indices.get(identity) != frame_index:
                self.pixel_card_frame_indices[identity] = frame_index
                self.message_render_cache.pop(identity, None)
                changed = True
        if changed:
            self._load_messages(preserve_scroll=True)

    @staticmethod
    def _day_marker(timestamp: int) -> tuple[tuple[int, int], Text]:
        local = time.localtime(timestamp)
        label = time.strftime("%A · %B", local) + f" {local.tm_mday}"
        return (local.tm_year, local.tm_yday), Text(
            f"────  {label}  ────", style="bold #718096")

    @staticmethod
    def _append_body_text(target: Text, value: str,
                          style: Style | str | None = "#d9e1ec") -> None:
        position = 0
        for mention in MENTION_RE.finditer(value):
            target.append(value[position:mention.start()], style=style)
            target.append(mention.group(0), style="bold #f9d56e")
            position = mention.end()
        target.append(value[position:], style=style)

    @classmethod
    def _render_inline_text(cls, body: str) -> Text:
        rendered = Text()
        position = 0
        for token in INLINE_TOKEN_RE.finditer(body):
            cls._append_body_text(rendered, body[position:token.start()])
            kind = token.lastgroup
            raw = token.group(0)
            if kind == "link":
                link = re.fullmatch(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", raw)
                if link:
                    target = link.group(2).rstrip(".,!?;:")
                    suffix = link.group(2)[len(target):]
                    rendered.append(link.group(1), style=Style(
                        color="#a9b6ff", underline=True, link=target))
                    cls._append_body_text(rendered, suffix)
                else:
                    cls._append_body_text(rendered, raw)
            elif kind == "url":
                target = raw.rstrip(".,!?;:")
                rendered.append(target, style=Style(
                    color="#a9b6ff", underline=True, link=target))
                cls._append_body_text(rendered, raw[len(target):])
            elif kind == "code":
                rendered.append(raw[1:-1], style="#f3ce82 on #202b3b")
            elif kind == "bold":
                cls._append_body_text(rendered, raw[2:-2], "bold #d9e1ec")
            elif kind == "underline":
                cls._append_body_text(rendered, raw[2:-2], "underline #d9e1ec")
            elif kind == "strike":
                cls._append_body_text(rendered, raw[2:-2], "strike #8a98ae")
            elif kind in {"italic", "italic_underscore"}:
                cls._append_body_text(rendered, raw[1:-1], "italic #d9e1ec")
            position = token.end()
        cls._append_body_text(rendered, body[position:])
        return rendered

    @classmethod
    def _render_inline_body(cls, body: str) -> Text:
        rendered = Text()
        for line in body.splitlines(keepends=True):
            content = line[:-1] if line.endswith("\n") else line
            newline = "\n" if line.endswith("\n") else ""
            heading = re.match(r"^(#{1,3}) +(.*)$", content)
            quote = re.match(r"^> ?(.*)$", content)
            bullet = re.match(r"^[*-] +(.*)$", content)
            numbered = re.match(r"^(\d+[.)]) +(.*)$", content)
            if heading:
                rendered.append(heading.group(1) + " ", style="#7566ad")
                heading_start = len(rendered.plain)
                rendered.append_text(cls._render_inline_text(heading.group(2)))
                heading_style = {"#": "bold #d8ccff", "##": "bold #c4b5fd",
                                 "###": "bold #b4a7d6"}[heading.group(1)]
                rendered.stylize(heading_style, heading_start, len(rendered.plain))
                for mention in MENTION_RE.finditer(rendered.plain, heading_start):
                    rendered.stylize("bold #f9d56e", mention.start(), mention.end())
            elif quote:
                rendered.append("│ ", style="#718096")
                quoted = cls._render_inline_text(quote.group(1))
                quoted.stylize("italic", 0, len(quoted.plain))
                rendered.append_text(quoted)
            elif bullet:
                rendered.append("• ", style="#79d7b2")
                rendered.append_text(cls._render_inline_text(bullet.group(1)))
            elif numbered:
                rendered.append(numbered.group(1) + " ", style="#79d7b2")
                rendered.append_text(cls._render_inline_text(numbered.group(2)))
            else:
                rendered.append_text(cls._render_inline_text(content))
            rendered.append(newline)
        return rendered

    @classmethod
    def _render_message_body(cls, body: str) -> list[Text | Syntax]:
        pieces: list[Text | Syntax] = []
        position = 0
        for block in FENCED_CODE_RE.finditer(body):
            before = body[position:block.start()]
            if before:
                before = before[:-1] if before.endswith("\n") else before
                if before:
                    pieces.append(cls._render_inline_body(before))
            language = block.group("language") or "text"
            try:
                get_lexer_by_name(language)
            except ClassNotFound:
                language = "text"
            code = block.group("code").rstrip("\r\n")
            pieces.append(Syntax(code, language, theme="monokai", word_wrap=True,
                                 line_numbers=False))
            position = block.end()
            if body[position:position + 1] == "\n":
                position += 1
        tail = body[position:]
        if tail:
            pieces.append(cls._render_inline_body(tail))
        if not pieces:
            pieces.append(cls._render_inline_body(body))
        return pieces

    def _linked_card_animation(self, second: PixelCard, message: dict
                               ) -> PixelCardAnimation | None:
        link = second.animation_link
        if not link:
            return None
        for candidate in self.chat.store.history(message["c"], self.history_limit):
            if (candidate["o"], int(candidate["s"])) == (message["o"], int(message["s"])):
                continue
            reference = decode_pixel_card_reference(str(candidate["b"]))
            if (reference is None or reference.is_linked_frame or
                    reference.card_id != link["first_card_id"] or
                    reference.callsign.casefold() != second.callsign.casefold() or
                    (reference.width, reference.height) != (second.width, second.height)):
                continue
            payload = self.chat.store.attachment_blob_for(reference.digest)
            if payload is None:
                continue
            try:
                first = verify_pixel_card_payload(payload, reference)
                if (first.animation_link is not None or
                        pixelqso_artwork_digest(first) != link["first_digest"]):
                    continue
            except ValueError:
                continue
            return PixelCardAnimation(second.title, first, second, link["frame_ms"],
                                      link["first_digest"], category=second.category)
        return None

    def _message_content(self, message: dict):
        stamp = time.strftime("%H:%M", time.localtime(int(message["ts"])))
        local = message["o"] == self.chat.store.node
        text = Text()
        text.append(f"{stamp}  ", style="#718096")
        path = "local" if local else self.chat.store.reception_for(message["o"], int(message["s"]))
        color, symbol = {"local": ("#c4b5fd", "●"), "direct": ("#8bd5ca", "●"),
                         "relayed": ("#e7b774", "↪"),
                         "unknown": ("#a8b2c3", "?")}[path]
        text.append(f"{symbol} {message['n']}", style=f"bold {color}")
        text.append("  ", style="#65748b")
        body = str(message["b"])
        parts: list[Text | Syntax] = [text]

        def highlight_search(body_part: Text) -> None:
            if self.search_query:
                for match in re.finditer(re.escape(self.search_query), body_part.plain,
                                         re.IGNORECASE):
                    body_part.stylize("bold #f9d56e", match.start(), match.end())

        card: PixelCard | PixelCardAnimation | None = None
        card_reference: PixelCardReference | None = None
        invalid_card = False
        if body.startswith(CARD_REFERENCE_PREFIX):
            try:
                card_reference = decode_pixel_card_reference(body)
                if card_reference is not None:
                    payload = self.chat.store.attachment_blob_for(card_reference.digest)
                    if payload is not None:
                        card = verify_pixel_card_payload(payload, card_reference)
            except ValueError:
                invalid_card = True
        elif body.startswith((CARD_MESSAGE_PREFIX, ANIMATION_MESSAGE_PREFIX)):
            try:
                card = decode_pixel_card_message(body)
            except ValueError:
                invalid_card = True
        if isinstance(card, PixelCard) and card.animation_link:
            card = self._linked_card_animation(card, message) or card
        if card is not None:
            identity = (message["o"], int(message["s"]))
            if isinstance(card, PixelCardAnimation):
                frame_index = card.frame_index(self._card_animation_now_ms())
                self.pixel_card_animations[identity] = card
                self.pixel_card_frame_indices[identity] = frame_index
                displayed_card = card.frames[frame_index]
                text.append(f"PixelQSO animation · {card.title}", style="bold #d8ccff")
            else:
                self.pixel_card_animations.pop(identity, None)
                self.pixel_card_frame_indices.pop(identity, None)
                frame_index = None
                displayed_card = card
                text.append(f"PixelQSO card · {card.title}", style="bold #d8ccff")
            details = Text("\n          " +
                           " · ".join(value for value in
                                      (displayed_card.callsign, displayed_card.grid,
                                       f"{displayed_card.width}×{displayed_card.height}",
                                       f"{len(displayed_card.palette)} colors") if value),
                           style="#9fb0c7")
            if isinstance(card, PixelCardAnimation):
                details.append(f" · frame {frame_index + 1}/2 · {card.frame_ms} ms/frame",
                               style="#d2ae67")
            elif card.animated:
                link = card.animation_link or {}
                details.append(f" · frame 2 → {link.get('first_card_id', 0):04X} · "
                               "first frame not collected", style="#d2ae67")
            parts.extend((details, render_pixel_card(displayed_card)))
        elif card_reference is not None and not invalid_card:
            ref_frame = card_reference.frames[0]
            text.append(f"PixelQSO card · {card_reference.title}", style="bold #d8ccff")
            details = Text("\n          " + " · ".join(value for value in
                           (ref_frame.callsign, ref_frame.grid,
                            f"{ref_frame.width}×{ref_frame.height}",
                            f"{len(ref_frame.palette)} colors") if value),
                           style="#9fb0c7")
            if card_reference.is_linked_frame:
                details.append(f" · frame 2 → {ref_frame.animation_link['first_card_id']:04X} · "
                               f"{card_reference.frame_ms} ms/frame", style="#d2ae67")
            details.append("\n          Not downloaded · request in Recent Attachments",
                           style="#d2ae67")
            parts.append(details)
        elif invalid_card:
            text.append("[invalid PixelQSO card reference/message]", style="bold #ff7d84")
        else:
            first_body_part = True
            for body_part in self._render_message_body(body):
                if isinstance(body_part, Text):
                    highlight_search(body_part)
                    if first_body_part:
                        text.append_text(body_part)
                    else:
                        parts.append(body_part)
                else:
                    parts.append(body_part)
                first_body_part = False

        footer = Text()

        def append_footer(value: str, style: Style | str) -> None:
            footer.append("\n          " if footer.plain else "          ", style="#718096")
            footer.append(value, style=style)

        if local:
            receipts = self.chat.store.receipts_for(message["o"], message["s"])
            mentions = [mention_name(match) for match in MENTION.finditer(body)]
            if mentions:
                states = []
                for mention in mentions:
                    stations = sorted({station for station, alias in receipts
                                       if alias.casefold() == mention.casefold()})
                    label = format_mention(mention)
                    states.append(f"✓ {label} · {', '.join(stations)}" if stations
                                  else f"… {label}")
                color = "#79d7b2" if all(state.startswith("✓") for state in states) else "#d2ae67"
                append_footer("  ·  ".join(states), color)
        auth = self.chat.store.auth_for(message["o"], message["s"])
        if auth["status"] == "verified":
            append_footer(f"✓ Verified · {auth['signer']}", "bold #79d7b2")
            if auth.get("reference"):
                append_footer(auth["reference"], "#7f8793")
        elif auth["status"] == "signing_failed":
            append_footer("! Sent unsigned · ARAP signing failed; will retry", "bold #ffb86c")
        elif auth["status"] == "pending":
            if auth["proof"] is None:
                append_footer("⟳ Resolving ARAP proof" if auth.get("reference")
                              else "⟳ Signing with ARAP", "#a9a0d0")
            else:
                append_footer("⟳ Checking ARAP signature", "#a9a0d0")
        elif auth["status"] == "untrusted":
            append_footer(f"◌ Signed · {auth['signer']} · key not trusted", "#d2ae67")
        elif auth["status"] == "unverified":
            append_footer(f"◌ Signature unchecked · claimed signer {auth['signer']}",
                          "#d2ae67")
        elif auth["status"] == "invalid":
            append_footer("! Signature invalid", "bold #ff7d84")
        attachment = message.get("image") or self.chat.store.attachment_for(
            message["o"], int(message["s"]))
        is_image = attachment and attachment.get("kind", "image") == "image"
        thumbnail = (self.chat.store.image_for(attachment["digest"])
                     if is_image else None)
        if is_image and not thumbnail:
            append_footer(f"▧ Image · {attachment['width']}×{attachment['height']} · "
                          "select it in Recent Attachments to request", "#8a98ae")
        if footer.plain:
            parts.append(footer)
        if thumbnail:
            parts.append(render_thumbnail(thumbnail))
        return parts[0] if len(parts) == 1 else Group(*parts)

    def _message_renderable(self, message: dict):
        identity = (message["o"], int(message["s"]))
        # Fading changes only the background. Keep image/body rendering and
        # durable status queries out of repeated animation updates.
        animation = self.pixel_card_animations.get(identity)
        if animation is not None:
            frame_index = animation.frame_index(self._card_animation_now_ms())
            if self.pixel_card_frame_indices.get(identity) != frame_index:
                self.pixel_card_frame_indices[identity] = frame_index
                self.message_render_cache.pop(identity, None)
        rendered = self.message_render_cache.get(identity)
        if rendered is None:
            rendered = self._message_content(message)
            self.message_render_cache[identity] = rendered
        arrived = self.new_message_times.get((message["o"], int(message["s"])))
        if arrived is not None:
            strength = max(0, 1 - (time.monotonic() - arrived) / MESSAGE_HIGHLIGHT_SECONDS)
            if strength:
                base, bright = (11, 17, 27), (36, 57, 72)
                background = '#' + ''.join(f'{round(a + (b - a) * strength):02x}'
                                            for a, b in zip(base, bright))
                return Padding(rendered, 0, style=Style(bgcolor=background))
        return rendered

    def _drain_events(self) -> None:
        # Textual may run an already-scheduled interval once while the app is
        # being detached. The screen is no longer available for UI updates.
        if not self.is_mounted:
            return
        changed_channels = False
        changed_messages = False
        image_progress_changed = False
        while True:
            try:
                event, value = self.chat.events.get_nowait()
            except queue.Empty:
                break
            if event in {"receipt", "auth", "auth-policy", "station-auth", "image", "attachment",
                         "reception", "refresh"}:
                self.message_render_cache.clear()
            if event == "message":
                identity = (value["o"], int(value["s"]))
                self.new_message_times.setdefault(identity, time.monotonic())
                if value["c"] == self.channel:
                    changed_messages = True
                else:
                    channel = value["c"]
                    unread = self.chat.store.unread_summary(channel)
                    if unread:
                        self.unread[channel] = int(unread[0]["messages"])
                        self.unread_mentions[channel] = int(unread[0]["mentions"] or 0)
                    else:
                        self.unread.pop(channel, None)
                        self.unread_mentions.pop(channel, None)
                changed_channels = True
                self._refresh_attachments()
                self._refresh_stats()
            elif event == "reception":
                changed_messages = True
            elif event == "receipt":
                changed_messages = True
                self.last_status = f"{format_mention(value['m'])} received by {value['n']}"
                self.query_one("#activity", Static).update(self.last_status)
            elif event == "auth":
                changed_messages = True
                if value["status"] == "verified":
                    self.last_status = f"Verified message from {value['signer']}"
                elif value["status"] == "invalid":
                    self.last_status = "Message signature failed verification"
                elif value["status"] == "unverified":
                    self.last_status = (f"Signature from {value['signer']} could not be checked")
                elif value["status"] == "signing_failed":
                    self.last_status = "Message sent unsigned; ARAP signing failed and will retry"
                else:
                    self.last_status = f"Signature from {value['signer']} is not trusted"
                self.query_one("#activity", Static).update(self._short_status(self.last_status))
            elif event == "auth-policy":
                changed_messages = True
                count = int(value.get("count", 0))
                self.last_status = f"ARAP policy changed; checking {count} signed messages"
                self.query_one("#activity", Static).update(self._short_status(self.last_status))
            elif event == "station-auth":
                self._refresh_stations()
                self.last_status = (f"ARAP authentication verified for {value['callsign']}; "
                                    "status remains until local trust changes")
                self.query_one("#activity", Static).update(self._short_status(self.last_status))
            elif event in {"image", "attachment"}:
                changed_messages = True
                self._refresh_attachments()
                if event == "image":
                    self.last_status = "Requested thumbnail received"
                    self.query_one("#activity", Static).update(self.last_status)
            elif event == "image-progress":
                image_progress_changed = True
            elif event == "refresh":
                # The bounded async event buffer overflowed. Rebuild the view
                # from SQLite, which is the durable source of message state.
                self._load_messages()
                self.chat.store.clear_unread(self.channel)
                self.unread.clear()
                self.unread_mentions.clear()
                for unread in self.chat.store.unread_summary():
                    if unread["c"] != self.channel:
                        self.unread[unread["c"]] = int(unread["messages"])
                        self.unread_mentions[unread["c"]] = int(unread["mentions"] or 0)
                changed_channels = True
                self._refresh_attachments()
                self._refresh_stats()
                self.last_status = "Event backlog coalesced; refreshed from stored history"
                self.query_one("#activity", Static).update(self._short_status(self.last_status))
            elif event == "status":
                self.last_status = str(value)
                self.query_one("#activity", Static).update(self._short_status(str(value)))
                if (str(value).startswith("Connected to Data2G") or
                        str(value).startswith("Connected to raw KISS")):
                    self.query_one("#link-detail", Static).update(self._link_detail())
                    if self.chat.args.transport == "broadcast":
                        mode_select = self.query_one("#broadcast-mode", Select)
                        mode_select.set_options([
                            ("Auto (widest under limit)", ""),
                            *((f"{name} · {width:g} Hz", name)
                              for name, width in self.chat.data.mode_catalog)])
                        mode_select.value = self.chat.data.manual_mode or ""
                        mode_select.disabled = not bool(self.chat.data.mode_catalog)
                        self.query_one("#broadcast-bandwidth", Select).disabled = not bool(
                            self.chat.data.mode_catalog)
                        self.query_one("#unattended-toggle", Checkbox).disabled = (
                            not self.chat.data.unattended_available)
                status = str(value).lower()
                link_state = self.query_one("#link-state", Static)
                if self.chat.args.transport == "connected":
                    if status.startswith("arq connected"):
                        link_state.update("● ARQ CONNECTED")
                    elif status.startswith("arq disconnected"):
                        link_state.update("● LISTENING")
                    elif status.startswith("connected to data2g"):
                        link_state.update("● LISTENING")
                    elif (status.startswith("data2g disconnected") or
                          status.startswith("data2g connection failed") or
                          "connection closed" in status or "connection error" in status or
                          "retrying" in status):
                        link_state.update("● RECONNECTING")
                elif "disconnected" in status or "closed" in status or "error" in status or "retrying" in status:
                    link_state.update("● RECONNECTING")
                elif status.startswith("connected to data2g"):
                    link_state.update("● ONLINE")
                elif status.startswith("connected to raw kiss"):
                    link_state.update("● ONLINE")
        if changed_channels:
            self._refresh_channels()
        if changed_messages:
            self._load_messages(preserve_scroll=True)
        if image_progress_changed:
            self._refresh_attachments()
        self._refresh_stations()
        self._refresh_tx_state()

    def _refresh_tx_state(self) -> None:
        snapshot = self.chat.data.transmit_snapshot()
        queued = snapshot["queued"]
        waiting = snapshot["pending_acks"]
        state = snapshot["state"]
        if state != "idle":
            label = f"● {state} · {snapshot['age']}s"
            style = "bold #ffb86c" if state == "write failed" else "#d2ae67"
        elif queued:
            label = f"● QUEUED · {queued} frame{'s' if queued != 1 else ''}"
            style = "bold #ffb86c"
        elif waiting:
            label = (f"● HOST RECEIPT · {waiting} pending · "
                     f"oldest {snapshot['oldest_ack']}s\nRF delivery unconfirmed")
            style = "#d2ae67"
        elif snapshot["transport"] == "ax25" and snapshot["last_submit"]:
            elapsed = max(0, int(time.monotonic() - snapshot["last_submit"]))
            label = f"● SUBMITTED TO KISS · {elapsed}s ago\nRF delivery unconfirmed"
            style = "#a9d8c5"
        else:
            label = "● IDLE · no frames queued"
            style = "#79d7b2"
        # The periodic callback can run while its screen is unmounting.
        widgets = self.query("#tx-state")
        if widgets:
            widgets.first(Static).update(Text(label, style=style))

    def _short_status(self, value: str) -> str:
        return value if len(value) <= 72 else value[:69] + "…"

    def _link_detail(self) -> str:
        if self.chat.args.transport == "ax25":
            path = ",".join(self.chat.data.digipeaters) or "direct"
            return (f"{self.chat.args.host}:{self.chat.data.kiss_port}\n"
                    f"AX.25 UI · {self.chat.args.callsign} → "
                    f"{self.chat.data.ax25_destination} · {path}")
        control = f"{self.chat.args.host}:{self.chat.data.command_port}"
        if self.chat.args.transport == "broadcast":
            return f"{control}\nKISS port {self.chat.data.port}"
        return (f"{control}\nVARA data {self.chat.data.command_port + 1} · "
                f"{self.chat.data.bandwidth} Hz cap")

    def _mention_targets(self) -> list[str]:
        values = {self.chat.args.callsign, self.chat.nick, *self.chat.station_names()}
        unique = {}
        for name in values:
            if valid_mention_name(name):
                unique.setdefault(name.casefold(), name)
        return sorted(unique.values(), key=str.casefold)

    def _connect_targets(self) -> list[str]:
        if self.chat.args.transport != "connected":
            return []
        own_call = self.chat.args.callsign.casefold()
        return sorted({callsign for _, callsign in self.chat.station_labels()
                       if callsign.casefold() != own_call}, key=str.casefold)

    def _refresh_stations(self) -> None:
        stations = self.chat.station_labels()
        authenticated = self.chat.store.authenticated_stations()
        lines = []
        for nick, callsign in stations:
            label = (f"●  {escape(nick)} · {escape(callsign)}"
                     if nick.casefold() != callsign.casefold()
                     else f"●  {escape(callsign)}")
            proof = authenticated.get(callsign.upper())
            if proof:
                label += "  [dim green]✓ authenticated[/]"
            lines.append(label)
        content = ("\n".join(lines)
                   if stations else "Listening for stations…")
        try:
            self.query_one("#station-list", Static).update(content)
        except NoMatches:
            # The interval can fire after Textual has removed the base screen
            # during shutdown. There is no station panel to update then.
            return

    def _refresh_attachments(self) -> None:
        self.attachments = self.chat.store.recent_attachments(self.channel)
        listing = self.query_one("#image-list", OptionList)
        listing.clear_options()
        options = []
        for index, attachment in enumerate(self.attachments):
            stamp = time.strftime("%H:%M", time.localtime(int(attachment["ts"])))
            cached = self.chat.store.attachment_blob_for(attachment["digest"]) is not None
            if cached:
                state = "✓"
            else:
                received, total = self.chat.store.image_progress(
                    attachment["origin"], int(attachment["seq"]))
                state = f"↻ {received}/{total}" if received else "↓"
            if attachment.get("kind", "image") == "card":
                try:
                    reference = decode_pixel_card_reference(attachment.get("metadata", ""))
                except ValueError:
                    reference = None
                title = reference.title if reference else "PixelQSO card"
                label = (f"{state}  Card · {escape(title)} · {stamp} · "
                         f"{attachment['width']}×{attachment['height']}")
            else:
                label = (f"{state}  Image · {escape(str(attachment['nick']))} · {stamp} · "
                         f"{attachment['width']}×{attachment['height']}")
            options.append(Option(label, id=f"image-{index}"))
        listing.add_options(options)
        if options:
            listing.highlighted = min(listing.highlighted or 0, len(options) - 1)
        request = self.query_one("#request-image", Button)
        request.disabled = not options
        request.label = "↓  Request selected"

    def _refresh_stats(self) -> None:
        count = self.chat.store.message_count()
        peers = self.chat.station_count()
        self.query_one("#sync-card", Static).update(
            f"{count:,} messages · {peers} station{'s' if peers != 1 else ''}\n"
            "Forwarding history nearby")


FENCED_CODE_RE = re.compile(
    r"(?m)^```(?P<language>[A-Za-z0-9_+#.-]{0,32})[ \t]*\r?\n"
    r"(?P<code>.*?)^```[ \t]*$", re.DOTALL)
INLINE_TOKEN_RE = re.compile(
    r"(?P<link>\[[^\]\n]{1,200}\]\(https?://[^)\s]+\))"
    r"|(?P<code>`[^`\n]+`)"
    r"|(?P<url>https?://[^\s<>\]]+)"
    r"|(?P<bold>\*\*[^*\n]+\*\*)"
    r"|(?P<underline>__[^_\n]+__)"
    r"|(?P<strike>~~[^~\n]+~~)"
    r"|(?P<italic>\*[^*\n]+\*)"
    r"|(?P<italic_underscore>(?<!\w)_[^_\n]+_(?!\w))"
)
