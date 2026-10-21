import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from textual.app import App, ComposeResult
from textual.widgets import Button, Input

from kisschat.app import Chat
from kisschat.tui import ChatApp, ImageAttachScreen


class ImageDialogHost(App[None]):
    def __init__(self):
        super().__init__()
        self.result = "not-dismissed"

    def compose(self) -> ComposeResult:
        yield Button("Open image dialog", id="open")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "open":
            self.push_screen(ImageAttachScreen(), self._record_result)

    def _record_result(self, value):
        self.result = value


class ImageAttachScreenTests(unittest.IsolatedAsyncioTestCase):
    async def test_visible_controls_path_enter_and_cancel_at_supported_sizes(self):
        for size in ((48, 24), (80, 24), (100, 30), (160, 45)):
            with self.subTest(size=size):
                app = ImageDialogHost()
                async with app.run_test(size=size) as pilot:
                    await pilot.click("#open")
                    await pilot.pause()

                    self.assertIsInstance(app.screen, ImageAttachScreen)
                    self.assertEqual(app.screen.query_one("#browse-image", Button).label,
                                     "Browse files")
                    self.assertEqual(app.screen.query_one("#send-image", Button).label,
                                     "Send image")
                    self.assertEqual(app.result, "not-dismissed")
                    for selector in ("#browse-image", "#send-image"):
                        button = app.screen.query_one(selector, Button)
                        self.assertGreater(button.region.width, 0)
                        self.assertGreater(button.region.height, 0)
                        self.assertGreaterEqual(button.region.x, 0)
                        self.assertLessEqual(button.region.right, size[0])
                        self.assertLessEqual(button.region.bottom, size[1])

                    field = app.screen.query_one("#image-path", Input)
                    field.value = "/tmp/test image.png"
                    field.focus()
                    await pilot.press("enter")
                    await pilot.pause()
                    self.assertEqual(app.result, ("/tmp/test image.png", ""))

                app = ImageDialogHost()
                async with app.run_test(size=size) as pilot:
                    await pilot.click("#open")
                    await pilot.pause()
                    await pilot.press("escape")
                    await pilot.pause()
                    self.assertIsNone(app.result)

                app = ImageDialogHost()
                async with app.run_test(size=size) as pilot:
                    await pilot.click("#open")
                    await pilot.pause()
                    app.screen.query_one("#image-path", Input).value = "/tmp/picture.png"
                    caption = app.screen.query_one("#image-caption", Input)
                    caption.value = "field test"
                    caption.focus()
                    await pilot.press("enter")
                    await pilot.pause()
                    self.assertEqual(app.result, ("/tmp/picture.png", "field test"))

    async def test_chat_app_queues_one_image_only_after_explicit_path_submit(self):
        with TemporaryDirectory() as directory:
            args = SimpleNamespace(
                host="127.0.0.1", callsign="N0CALL", nick=None, channel="#general",
                db=str(Path(directory) / "chat.sqlite3"), command_port=8300,
                kiss_port=None, mode="", transport="broadcast", bandwidth="2300",
                arap_cli="arap", arap_key="", arap_key_dir=str(Path(directory) / "keys"),
                arap_trust_store=str(Path(directory) / "trust.json"),
                arap_passphrase_env="KISSCHAT_ARAP_PASSPHRASE",
            )
            chat = Chat(args)
            queued = []
            chat.queue_image = lambda path, caption: queued.append((path, caption))
            app = ChatApp(chat)
            try:
                async with app.run_test(size=(100, 30)) as pilot:
                    await pilot.click("#attach-image")
                    await pilot.pause()
                    field = app.screen.query_one("#image-path", Input)
                    field.value = "/tmp/radio test.png"
                    field.focus()
                    self.assertEqual(queued, [])
                    await pilot.press("enter")
                    await pilot.pause()
                    self.assertEqual(queued, [("/tmp/radio test.png", "")])
            finally:
                chat.close()

    async def test_invalid_path_reports_failure_without_creating_or_sending_message(self):
        with TemporaryDirectory() as directory:
            args = SimpleNamespace(
                host="127.0.0.1", callsign="N0CALL", nick=None, channel="#general",
                db=str(Path(directory) / "chat.sqlite3"), command_port=8300,
                kiss_port=None, mode="", transport="broadcast", bandwidth="2300",
                arap_cli="arap", arap_key="", arap_key_dir=str(Path(directory) / "keys"),
                arap_trust_store=str(Path(directory) / "trust.json"),
                arap_passphrase_env="KISSCHAT_ARAP_PASSPHRASE",
            )
            chat = Chat(args)
            app = ChatApp(chat)
            try:
                async with app.run_test(size=(100, 30)) as pilot:
                    await pilot.click("#attach-image")
                    await pilot.pause()
                    field = app.screen.query_one("#image-path", Input)
                    field.value = str(Path(directory) / "missing image.png")
                    field.focus()
                    await pilot.press("enter")
                    await pilot.pause(0.4)
                    self.assertIn("Image could not be attached",
                                  str(app.query_one("#activity").render()))
                    self.assertEqual(chat.store.message_count(), 0)
                    self.assertTrue(chat.data.outbound.empty())
                    self.assertTrue(chat.data.bulk_outbound.empty())
            finally:
                chat.close()


if __name__ == "__main__":
    unittest.main()
