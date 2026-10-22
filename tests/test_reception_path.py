"""Reception-path classification from on-air frame metadata."""
import tempfile
import time
import unittest

from kisschat.app import Chat
from kisschat.protocol import message_parts, unpack
from scripts.simulate_mesh import make_args


class RecordingRadio:
    port = 0
    manual_mode = None
    mode_catalog = ()

    def send(self, payload, *, priority=0):
        pass

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

    def receive_message(self, *, reception=None, recipient=""):
        frames = message_parts(origin=self.origin, seq=1, channel="#general",
                               nick="Alice", timestamp=int(time.time()),
                               text="heard directly", to=recipient)
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


if __name__ == "__main__":
    unittest.main()
