import tempfile
import unittest

from kisschat.store import Store


class StoreClearHistoryTests(unittest.TestCase):
    def test_purge_removes_history_and_preserves_non_reused_local_sequences(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(f"{directory}/chat.sqlite", "N0CALL")
            store.save_channel("#test")
            first = store.insert_local("#test", "N0CALL", 100, "first")
            store.insert("peer-node", 1, "#test", "PEER", 101, "relayed")
            store.set_auth(first["o"], first["s"], b"proof", "pending")

            self.assertEqual(store.clear_local_history(), 2)
            self.assertEqual(store.message_count(), 0)
            self.assertEqual(store.inventory(0), [])
            self.assertEqual(store.channels(), ["#test"])
            self.assertEqual(store.node, first["o"])
            self.assertEqual(store.highwater(store.node), 0)

            second = store.insert_local("#test", "N0CALL", 102, "after purge")
            self.assertEqual(second["s"], first["s"] + 1)
            self.assertEqual(store.contiguous(store.node), second["s"])
            self.assertEqual(store.inventory(0), [(store.node, second["s"], second["s"])])
            store.close()


if __name__ == "__main__":
    unittest.main()
