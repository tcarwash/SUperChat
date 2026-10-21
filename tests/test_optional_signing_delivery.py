import threading
import unittest
from types import SimpleNamespace

from kisschat.app import Chat, ChatEventQueue


class OptionalSigningDeliveryTests(unittest.TestCase):
    def test_signing_queue_failure_does_not_hold_message_delivery(self):
        order = []
        message = {"o": "01" * 16, "s": 7, "c": "#test", "n": "N0CALL",
                   "ts": 100, "b": "hello"}

        class Store:
            node = message["o"]

            def insert_local(self, channel, nick, timestamp, body, image, signer):
                order.append("stored")
                return dict(message)

        class Data:
            def send(self, frame):
                order.append("body-frame")

        chat = Chat.__new__(Chat)
        chat.channel = "#test"
        chat.nick = "N0CALL"
        chat.args = SimpleNamespace(callsign="N0CALL")
        chat.store = Store()
        chat.events = ChatEventQueue()
        chat.data = Data()
        chat.auth = SimpleNamespace(enabled=True)
        chat.pending_local_delivery = threading.Event()
        chat._queue_signature = lambda _message: order.append("sign-queue-full") or False
        chat.announce_origin = lambda *_args, **_kwargs: True

        chat.publish("hello")

        self.assertEqual(order[0], "stored")
        self.assertIn("body-frame", order)
        self.assertEqual(order[-1], "sign-queue-full")
        self.assertFalse(chat.pending_local_delivery.is_set())


if __name__ == "__main__":
    unittest.main()
