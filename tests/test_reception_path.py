"""Reception-path classification from on-air frame metadata."""
import tempfile
import time
import unittest

from kisschat.app import Chat
from kisschat.protocol import message_parts, relay_message_part, unpack
from scripts.simulate_mesh import make_args


class RecordingRadio:
    port = 0
    manual_mode = None
    mode_catalog = ()

    def __init__(self):
        self.sent = []

    def send(self, payload, *, priority=0):
        self.sent.append((payload, priority))

    def close(self):
        pass


class ReceptionPathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.chat = Chat(make_args("B", "Bob", self.temp.name + "/b.sqlite"))
        self.chat.data = RecordingRadio()
        self.addCleanup(self.chat.close)
        self.origin = "03" * 16

    def receive_message(self, *, reception=None, recipient="", sender=None,
                        text="heard directly"):
        frames = message_parts(origin=self.origin, seq=1, channel="#general",
                              nick="Alice", timestamp=int(time.time()),
                              text=text, to=recipient, sender=sender)
        for frame in frames:
            self.chat._on_frame(0, frame, reception)

    def test_direct_on_air_message_does_not_need_prior_inventory(self):
        self.receive_message(reception={"source": "K0AAA", "repeated": False})
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "direct")

    def test_digipeated_message_is_relayed(self):
        self.receive_message(reception={"source": "K0DIGI", "repeated": True})
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "relayed")

    def test_addressed_history_does_not_map_cache_to_message_origin(self):
        self.receive_message(reception={"source": "K0CACHE", "repeated": False},
                             recipient=self.chat.store.node)
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "unknown")

    def test_transport_without_source_metadata_stays_unknown(self):
        self.receive_message()
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "unknown")

    def test_data2g_origin_sender_metadata_marks_direct(self):
        self.receive_message(sender=self.origin)
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "direct")

    def test_data2g_cache_sender_metadata_marks_relayed(self):
        self.receive_message(sender="04" * 16)
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "relayed")

    def test_data2g_addressed_relay_replaces_sender_with_forwarder(self):
        original = message_parts(
            origin=self.origin, seq=1, channel="#general", nick="Alice",
            timestamp=int(time.time()), text="history", to=self.chat.store.node,
            sender=self.origin)
        part = unpack(original[0])
        forwarder = "04" * 16
        relayed = relay_message_part(part, 2, sender=forwarder)
        self.chat._on_frame(0, relayed)
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "relayed")

    def test_long_data2g_message_assembles_with_sender_header(self):
        body = "0123456789" * 80
        self.receive_message(sender=self.origin, text=body)
        self.assertEqual(self.chat.store.history("#general")[0]["b"], body)
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "direct")

    def test_maximum_channel_and_nick_fit_sender_tagged_data2g_message(self):
        channel, nick, body = "#" + "a" * 31, "B" * 16, "x" * 4096
        frames = message_parts(origin=self.origin, seq=1, channel=channel, nick=nick,
                               timestamp=int(time.time()), text=body, sender=self.origin)
        self.assertTrue(all(len(frame) <= 220 for frame in frames))
        for frame in frames:
            self.chat._on_frame(0, frame)
        self.assertEqual(self.chat.store.history(channel)[0]["b"], body)
        self.assertEqual(self.chat.store.reception_for(self.origin, 1), "direct")

    def test_publish_emits_local_node_as_data2g_sender(self):
        self.chat.publish("live")
        data_parts = [unpack(payload) for payload, _ in self.chat.data.sent]
        data_parts = [part for part in data_parts if part and part.get("t") == "d"]
        self.assertEqual(len(data_parts), 1)
        self.assertEqual(data_parts[0]["from"], self.chat.store.node)


if __name__ == "__main__":
    unittest.main()
