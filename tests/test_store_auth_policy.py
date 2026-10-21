import tempfile
import unittest

from kisschat.store import Store


class StoreAuthPolicyTests(unittest.TestCase):
    def test_revision_change_invalidates_old_decisions_but_preserves_proofs(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(f"{directory}/chat.sqlite", "N0CALL")
            message = store.insert_local("#test", "N0CALL", 100, "signed")
            store.set_auth(message["o"], message["s"], b"proof", "verified", "N0CALL")
            revision_a = "a" * 64
            revision_b = "b" * 64
            self.assertEqual(store.refresh_auth_policy(revision_a), 1)
            self.assertEqual(store.auth_for(message["o"], message["s"])["status"], "pending")
            self.assertEqual(store.proof_for(message["o"], message["s"]), b"proof")
            self.assertEqual(store.refresh_auth_policy(revision_a), 0)
            store.set_auth(message["o"], message["s"], b"proof", "verified", "N0CALL")
            self.assertEqual(store.refresh_auth_policy(revision_b), 1)
            self.assertEqual(store.auth_for(message["o"], message["s"])["status"], "pending")
            store.close()


if __name__ == "__main__":
    unittest.main()
