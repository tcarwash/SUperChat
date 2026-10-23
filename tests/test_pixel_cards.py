"""PixelQSO card message encoding and local-library discovery."""
import base64
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kisschat.pixel_cards import (decode_pixel_card_message, encode_pixel_card_message,
                                  list_pixel_cards, load_pixel_card, PixelCard,
                                  PixelCardAnimation, pixelqso_artwork_digest,
                                  save_received_pixel_card_message,
                                  decode_pixel_card_payload,
                                  decode_pixel_card_reference,
                                  encode_pixel_card_payload,
                                  encode_pixel_card_reference,
                                  verify_pixel_card_payload)
from kisschat.protocol import MAX_MESSAGE_BYTES, unpack
from kisschat.store import Store


def card_document(*, width=16, height=16, palette=None, pixels=None):
    palette = palette or [[0, 0, 0], [15, 15, 15]]
    pixels = pixels if pixels is not None else [index % len(palette)
                                                 for index in range(width * height)]
    return {"callsign": "N0CALL", "grid": "FN31", "palette": palette,
            "pixels": pixels, "card_id": 0x1234, "width": width,
            "height": height, "animation": None,
            "base_pixels": pixels, "stamp_layers": []}


def linked_card_library(root):
    palette = [[0, 0, 0], [15, 15, 15], [0, 0, 15], [0, 15, 15],
               [0, 15, 0], [15, 15, 0], [15, 0, 0], [15, 0, 15]]
    folder = root / "examples" / "starter-cards"
    folder.mkdir(parents=True)
    first_data = card_document(palette=palette,
                               pixels=[index % 8 for index in range(16 * 16)])
    first_data["card_id"] = 100
    first_path = folder / "signal-first.json"
    first_path.write_text(json.dumps(first_data))
    first = load_pixel_card(first_path)
    second_data = card_document(palette=palette,
                                pixels=[(index + 1) % 8 for index in range(16 * 16)])
    second_data["card_id"] = 101
    second_data["animation"] = {
        "first_card_id": first.card_id,
        "first_digest": pixelqso_artwork_digest(first),
        "frame_ms": 250,
    }
    (folder / "signal-second.json").write_text(json.dumps(second_data))
    return first_data, second_data


def deliver_new_chat_messages(source, target, start: int = 0) -> int:
    for payload, _ in source.data.sent[start:]:
        record = unpack(payload)
        if record and record.get("t") == "d":
            target._on_frame(0, payload)
    return len(source.data.sent)


def request_deferred_attachment(source, target, attachment) -> None:
    request_start = len(target.data.sent)
    target.command(f"/image-get {attachment['origin']}:{attachment['seq']}")
    requests = [payload for payload, _ in target.data.sent[request_start:]
                if (record := unpack(payload)) and record.get("t") == "g"]
    for request in requests:
        response_start = len(source.data.sent)

        def respond(request_key, recipient, metadata, body, indexes=None):
            source._send_image(recipient, metadata["origin"], int(metadata["seq"]),
                               metadata["digest"], body, request_key[-1], indexes)

        with patch.object(source, "_schedule_image_response", side_effect=respond):
            source._on_frame(0, request)
        for payload, _ in source.data.sent[response_start:]:
            record = unpack(payload)
            if record and record.get("t") == "x":
                target._on_frame(0, payload)


class PixelCardTests(unittest.TestCase):
    def test_pixelqso_card_message_round_trips_as_a_distinct_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "night-signal.json"
            path.write_text(json.dumps(card_document()))
            card = load_pixel_card(path)
            message = encode_pixel_card_message(card)
            decoded = decode_pixel_card_message(message)

        self.assertTrue(message.startswith("KISSCHAT-CARD/1:"))
        self.assertLessEqual(len(message.encode("utf-8")), MAX_MESSAGE_BYTES)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.title, "night-signal")
        self.assertEqual(decoded.callsign, "N0CALL")
        self.assertEqual(decoded.grid, "FN31")
        self.assertEqual(decoded.card_id, 0x1234)
        self.assertEqual(decoded.palette, card.palette)
        self.assertEqual(decoded.pixels, card.pixels)
        self.assertIsNone(decode_pixel_card_message("ordinary chat text"))

    def test_maximum_supported_card_fits_the_chat_message_limit(self):
        palette = [[index & 15, (index >> 1) & 15, (index >> 2) & 15]
                   for index in range(32)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "large-card.json"
            path.write_text(json.dumps(card_document(
                width=64, height=64, palette=palette,
                pixels=[index % 32 for index in range(64 * 64)])))
            message = encode_pixel_card_message(load_pixel_card(path))

        self.assertLessEqual(len(message.encode("utf-8")), MAX_MESSAGE_BYTES)
        self.assertEqual(decode_pixel_card_message(message).width, 64)

    def test_card_library_includes_personal_sent_and_pixelqso_starter_cards(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data"
            source = root / "pixelqso"
            files = [
                (data / "cards" / "personal.json", "my"),
                (data / "sent-cards" / "sent.json", "sent"),
                (source / "examples" / "starter-cards" / "starter.json", "starter"),
            ]
            for path, _ in files:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(card_document()))

            cards = list_pixel_cards(data_dir=data, source_dir=source)

        self.assertEqual({card.category for card in cards}, {"my", "sent", "starter"})
        self.assertEqual({card.title for card in cards}, {"personal", "sent", "starter"})

    def test_invalid_pixelqso_cards_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            path.write_text(json.dumps(card_document(pixels=[9] * 256)))
            with self.assertRaises(ValueError):
                load_pixel_card(path)

    def test_card_message_crc_covers_raster_and_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "crc.json"
            path.write_text(json.dumps(card_document()))
            message = encode_pixel_card_message(load_pixel_card(path))
        payload = bytearray(base64.urlsafe_b64decode(
            message.removeprefix("KISSCHAT-CARD/1:")))
        payload[8] ^= 1
        corrupt = "KISSCHAT-CARD/1:" + base64.urlsafe_b64encode(payload).decode("ascii")
        with self.assertRaises(ValueError):
            decode_pixel_card_message(corrupt)

    def test_linked_cards_are_independent_messages_and_received_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_data, second_data = linked_card_library(root / "pixelqso")
            cards = list_pixel_cards(data_dir=root / "empty", source_dir=root / "pixelqso")
            first = next(card for card in cards if card.card_id == 100)
            second = next(card for card in cards if card.card_id == 101)
            message1, payload1, reference1 = encode_pixel_card_reference(first)
            message2, payload2, reference2 = encode_pixel_card_reference(second)
            decoded1 = verify_pixel_card_payload(payload1, decode_pixel_card_reference(message1))
            decoded2 = verify_pixel_card_payload(payload2, decode_pixel_card_reference(message2))
            corrupt_payload = bytearray(payload2)
            corrupt_payload[-1] ^= 1
            with self.assertRaises(ValueError):
                verify_pixel_card_payload(bytes(corrupt_payload), reference2)
            saved1 = save_received_pixel_card_message(
                decoded1, origin="ab" * 16, seq=9, data_dir=root / "receiver-pixelqso")
            saved2 = save_received_pixel_card_message(
                decoded2, origin="ab" * 16, seq=10, data_dir=root / "receiver-pixelqso")
            received_logs = [json.loads(path.read_text()) for path in
                             sorted((root / "receiver-pixelqso" / "qso-log").glob("*.json"))]
            saved_logs = list((root / "receiver-pixelqso" / "qso-log").glob("*.json"))

        self.assertTrue(message1.startswith("KISSCHAT-CARD-REF/1:"))
        self.assertTrue(message2.startswith("KISSCHAT-CARD-REF/1:"))
        self.assertEqual(reference1.frames[0].card_id, 100)
        self.assertEqual(reference2.frames[0].card_id, 101)
        self.assertEqual(reference2.frames[0].animation_link, second_data["animation"])
        self.assertEqual(decoded1.pixels, tuple(first_data["pixels"]))
        self.assertEqual(decoded2.animation_link, second_data["animation"])
        self.assertEqual([log["received_cards"][0]["card"]["card_id"]
                          for log in received_logs], [100, 101])
        self.assertEqual(received_logs[1]["received_cards"][0]["card"]["animation"],
                         second_data["animation"])
        self.assertEqual({log["source_message"]["seq"] for log in received_logs}, {9, 10})
        self.assertEqual(len(saved_logs), 2)

    def test_card_reference_keeps_log_metadata_separate_from_lazy_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, second_data = linked_card_library(root / "pixelqso")
            second = next(card for card in list_pixel_cards(
                data_dir=root / "empty", source_dir=root / "pixelqso")
                if card.card_id == 101)
            message_body, payload, reference = encode_pixel_card_reference(second)
            decoded_reference = decode_pixel_card_reference(message_body)
            descriptor = json.loads(base64.urlsafe_b64decode(
                message_body.removeprefix("KISSCHAT-CARD-REF/1:")))
            card = verify_pixel_card_payload(payload, decoded_reference)

        self.assertTrue(message_body.startswith("KISSCHAT-CARD-REF/1:"))
        self.assertLessEqual(len(message_body.encode("utf-8")), MAX_MESSAGE_BYTES)
        self.assertIsNotNone(decoded_reference)
        self.assertEqual(decoded_reference.digest, reference.digest)
        self.assertEqual(decoded_reference.size, len(payload))
        self.assertEqual([frame.title for frame in decoded_reference.frames], ["signal-second"])
        self.assertEqual(decoded_reference.frame_ms, 250)
        self.assertEqual(descriptor["frames"][0]["animation"], second_data["animation"])
        self.assertNotIn("pixels", descriptor["frames"][0])
        self.assertEqual(decode_pixel_card_payload(payload).pixels, second.pixels)
        self.assertEqual(card.animation_link, second_data["animation"])
        self.assertIsNone(decode_pixel_card_message(message_body))

    def test_local_card_message_stores_attachment_blob_without_putting_pixels_in_log_body(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            card_path = root / "card.json"
            card_path.write_text(json.dumps(card_document()))
            card = load_pixel_card(card_path)
            body, payload, reference = encode_pixel_card_reference(card)
            store = Store(str(root / "chat.sqlite"), "N0CALL")
            try:
                message = store.insert_local(
                    "#general", "N0CALL", 100, body,
                    card_attachment={"payload": payload, "width": card.width,
                                     "height": card.height, "digest": reference.digest,
                                     "metadata": body})
                attachment = store.attachment_for(message["o"], message["s"])
                recent = store.recent_attachments("#general")
                cached = store.attachment_blob_for(reference.digest)
            finally:
                store.close()

        self.assertTrue(message["b"].startswith("KISSCHAT-CARD-REF/1:"))
        self.assertEqual(attachment["kind"], "card")
        self.assertEqual(attachment["metadata"], body)
        self.assertEqual(cached, payload)
        self.assertEqual(recent[0]["kind"], "card")

class PixelCardPickerTests(unittest.IsolatedAsyncioTestCase):
    async def test_picker_shows_preview_for_highlighted_card_and_animation(self):
        from io import StringIO
        from rich.console import Console
        from scripts.simulate_mesh import make_args
        from kisschat.app import Chat
        from kisschat.tui import ChatApp, PixelCardPicker

        palette = ((0, 0, 0), (15, 15, 15), (0, 0, 15), (0, 15, 15),
                   (0, 15, 0), (15, 15, 0), (15, 0, 0), (15, 0, 15))
        first = PixelCard("first-frame", "N0CALL", "FN31", 100, 16, 16,
                          palette, tuple(index % 8 for index in range(256)),
                          category="my")
        digest = pixelqso_artwork_digest(first)
        second = PixelCard(
            "second-frame", "N0CALL", "FN31", 101, 16, 16, palette,
            tuple((index + 1) % 8 for index in range(256)), category="my",
            animated=True, animation_link={"first_card_id": 100,
                                           "first_digest": digest, "frame_ms": 250})
        with tempfile.TemporaryDirectory() as directory:
            chat = Chat(make_args("B", "Bob", str(Path(directory) / "chat.sqlite")))
            app = ChatApp(chat)
            try:
                with patch("kisschat.tui.list_pixel_cards", return_value=[first, second]):
                    async with app.run_test(size=(100, 30)) as pilot:
                        await pilot.click("#attach-card")
                        await pilot.pause()
                        picker = app.screen
                        self.assertIsInstance(picker, PixelCardPicker)
                        self.assertEqual(picker.previewed_index, 0)
                        output = StringIO()
                        Console(file=output, width=80, color_system=None).print(
                            picker._preview_renderable(picker.previewed_index))
                        self.assertIn("first-frame", output.getvalue())
                        self.assertIn("▀", output.getvalue())

                        await pilot.press("down")
                        await pilot.pause()
                        self.assertEqual(picker.previewed_index, 1)
                        output = StringIO()
                        Console(file=output, width=80, color_system=None).print(
                            picker._preview_renderable(picker.previewed_index))
                        self.assertIn("second-frame", output.getvalue())
                        self.assertIn("links to 0064", output.getvalue())
                        self.assertIn("250 ms/frame", output.getvalue())
                        self.assertIn("▀", output.getvalue())
            finally:
                chat.close()

    async def test_card_button_sends_card_entity_without_using_image_queue(self):
        from scripts.simulate_mesh import make_args
        from kisschat.app import Chat
        from kisschat.tui import ChatApp, PixelCardPicker

        for size in ((100, 30), (160, 45)):
            with self.subTest(size=size), tempfile.TemporaryDirectory() as directory:
                chat = Chat(make_args("B", "Bob", str(Path(directory) / "chat.sqlite")))
                queued_cards, queued_images = [], []
                chat.queue_card = lambda path: queued_cards.append(path)
                chat.queue_image = lambda path, caption: queued_images.append((path, caption))
                card_path = Path(directory) / "card.json"
                card = PixelCard("night-signal", "N0CALL", "FN31", 0x1234,
                                 16, 16, ((0, 0, 0), (15, 15, 15)), tuple([0] * 256),
                                 card_path, "my")
                app = ChatApp(chat)
                try:
                    with patch("kisschat.tui.list_pixel_cards", return_value=[card]):
                        async with app.run_test(size=size) as pilot:
                            clicked = await pilot.click("#attach-card")
                            await pilot.pause()
                            self.assertTrue(clicked)
                            self.assertIsInstance(app.screen, PixelCardPicker)
                            await pilot.press("enter")
                            await pilot.pause()
                    self.assertEqual(queued_cards, [card],
                                     f"card selection failed at terminal size {size}")
                    self.assertEqual(queued_images, [])
                finally:
                    chat.close()

    def test_card_is_logged_before_its_payload_is_requested(self):
        from io import StringIO
        from rich.console import Console
        from scripts.simulate_mesh import make_args
        from kisschat.app import Chat
        from kisschat.tui import ChatApp

        class Radio:
            port = 0
            manual_mode = None
            mode_catalog = ()

            def __init__(self):
                self.sent = []

            def send(self, payload, *, priority=0):
                self.sent.append((payload, priority))

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            card_path = Path(directory) / "night-signal.json"
            card_path.write_text(json.dumps(card_document()))
            sender = Chat(make_args("A", "Alice", str(Path(directory) / "a.sqlite")))
            receiver = Chat(make_args("B", "Bob", str(Path(directory) / "b.sqlite")))
            sender.data, receiver.data = Radio(), Radio()
            try:
                with patch.object(sender.image_executor, "submit",
                                  side_effect=lambda callback, *args: callback(*args)):
                    sender.queue_card(str(card_path))
                records = [unpack(payload) for payload, _ in sender.data.sent]
                self.assertFalse(any(record and record.get("t") == "x" for record in records))
                with patch.dict(os.environ, {"PIXELQSO_DATA_DIR":
                                             str(Path(directory) / "pixelqso-data")}):
                    deliver_new_chat_messages(sender, receiver)

                sent = sender.store.history("#general")[0]
                received = receiver.store.history("#general")[0]
                reference = decode_pixel_card_reference(sent["b"])
                self.assertEqual(reference.title, "night-signal")
                self.assertEqual(received["b"], sent["b"])
                attachment = receiver.store.attachment_for(received["o"], int(received["s"]))
                self.assertEqual(attachment["kind"], "card")
                self.assertEqual(receiver.store.recent_attachments("#general")[0]["kind"],
                                 "card")
                self.assertIsNone(receiver.store.attachment_blob_for(reference.digest))
                self.assertEqual(list((Path(directory) / "pixelqso-data" / "qso-log").glob("*.json")),
                                 [])
                output = StringIO()
                Console(file=output, width=80, color_system=None).print(
                    ChatApp(receiver)._message_content(received))
                rendered = output.getvalue()
                self.assertIn("PixelQSO card · night-signal", rendered)
                self.assertIn("Not downloaded", rendered)
                self.assertNotIn("▀", rendered)
                self.assertNotIn("KISSCHAT-CARD/1:", rendered)

                with patch.dict(os.environ, {"PIXELQSO_DATA_DIR":
                                             str(Path(directory) / "pixelqso-data")}):
                    request_deferred_attachment(sender, receiver, attachment)
                    received_qsos = list((Path(directory) / "pixelqso-data" / "qso-log").glob("*.json"))
                    self.assertEqual(len(received_qsos), 1)
                    qso_log = json.loads(received_qsos[0].read_text())
                    self.assertEqual(len(qso_log["received_cards"]), 1)
                    self.assertEqual(qso_log["received_cards"][0]["card"]["card_id"], 0x1234)
                    self.assertEqual(receiver.store.attachment_blob_for(reference.digest),
                                     sender.store.attachment_blob_for(reference.digest))
                output = StringIO()
                Console(file=output, width=80, color_system=None).print(
                    ChatApp(receiver)._message_content(received))
                self.assertIn("▀", output.getvalue())
            finally:
                sender.close()
                receiver.close()

    async def test_individually_requested_linked_cards_animate_only_when_both_are_collected(self):
        from scripts.simulate_mesh import make_args
        from kisschat.app import Chat
        from kisschat.tui import ChatApp

        class Radio:
            port = 0
            command_port = 8300
            manual_mode = None
            max_bandwidth = None
            mode_catalog = ()

            def __init__(self):
                self.sent = []

            def send(self, payload, *, priority=0):
                self.sent.append((payload, priority))

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "pixelqso-source"
            linked_card_library(source)
            cards = list_pixel_cards(data_dir=root / "sender-data", source_dir=source)
            first = next(card for card in cards if card.card_id == 100)
            second = next(card for card in cards if card.card_id == 101)
            sender = Chat(make_args("A", "Alice", str(root / "a.sqlite")))
            receiver = Chat(make_args("B", "Bob", str(root / "b.sqlite")))
            sender.data, receiver.data = Radio(), Radio()
            receiver_data = root / "receiver-pixelqso"
            for radio in (sender.data, receiver.data):
                radio.max_bandwidth = None
                radio.set_max_bandwidth = lambda value, target=radio: setattr(
                    target, "max_bandwidth", value)
                radio.set_broadcast_mode = lambda mode, target=radio: setattr(
                    target, "manual_mode", mode)
                radio.transmit_snapshot = lambda: {
                    "queued": 0, "pending_acks": 0, "state": "idle", "age": 0,
                    "oldest_ack": 0, "transport": "broadcast", "last_submit": 0,
                }
            try:
                with patch.object(sender.image_executor, "submit",
                                  side_effect=lambda callback, *args: callback(*args)):
                    sender.queue_card(first)
                cursor = deliver_new_chat_messages(sender, receiver)
                with patch.object(sender.image_executor, "submit",
                                  side_effect=lambda callback, *args: callback(*args)):
                    sender.queue_card(second)
                deliver_new_chat_messages(sender, receiver, cursor)
                records = [unpack(payload) for payload, _ in sender.data.sent]
                self.assertFalse(any(record and record.get("t") == "x" for record in records))
                with patch.dict(os.environ, {"PIXELQSO_DATA_DIR": str(receiver_data)}):
                    received = receiver.store.history("#general")
                    by_id = {decode_pixel_card_reference(row["b"]).card_id: row
                             for row in received}
                    self.assertEqual(set(by_id), {100, 101})
                    first_attachment = receiver.store.attachment_for(
                        by_id[100]["o"], int(by_id[100]["s"]))
                    second_attachment = receiver.store.attachment_for(
                        by_id[101]["o"], int(by_id[101]["s"]))
                    self.assertEqual(first_attachment["kind"], "card")
                    self.assertEqual(second_attachment["kind"], "card")
                    self.assertIsNone(receiver.store.attachment_blob_for(
                        first_attachment["digest"]))
                    self.assertIsNone(receiver.store.attachment_blob_for(
                        second_attachment["digest"]))
                    self.assertEqual(list((receiver_data / "qso-log").glob("*.json")), [])
                    request_deferred_attachment(sender, receiver, first_attachment)
                    self.assertEqual(len(list((receiver_data / "qso-log").glob("*.json"))), 1)
                    request_deferred_attachment(sender, receiver, second_attachment)
                qso_files = list((receiver_data / "qso-log").glob("*.json"))
                self.assertEqual(len(qso_files), 2)
                qso_cards = [json.loads(path.read_text())["received_cards"][0]["card"]
                             for path in qso_files]
                self.assertEqual({card["card_id"] for card in qso_cards}, {100, 101})
                second_qso = next(card for card in qso_cards if card["card_id"] == 101)
                self.assertEqual(second_qso["animation"]["first_card_id"], 100)

                app = ChatApp(receiver)
                async with app.run_test(size=(110, 35)) as pilot:
                    await pilot.pause()
                    self.assertEqual(len(app.attachments), 2)
                    self.assertTrue(all(item["kind"] == "card" for item in app.attachments))
                    received_second = by_id[101]
                    identity = (received_second["o"], int(received_second["s"]))
                    self.assertIn(identity, app.pixel_card_animations)
                    with patch.object(app, "_card_animation_now_ms", return_value=0):
                        app._load_messages()
                    self.assertEqual(app.pixel_card_frame_indices[identity], 0)
                    first_render = app.message_render_cache[identity]
                    with patch.object(app, "_card_animation_now_ms", return_value=250):
                        app._animate_pixel_card_messages()
                    self.assertEqual(app.pixel_card_frame_indices[identity], 1)
                    self.assertIsNot(app.message_render_cache[identity], first_render)
            finally:
                sender.close()
                receiver.close()


if __name__ == "__main__":
    unittest.main()
