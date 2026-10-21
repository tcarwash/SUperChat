import tempfile
import threading
import unittest
from types import SimpleNamespace

from kisschat.app import Chat
from kisschat.store import Store


class OptionalSigningRecoveryTests(unittest.TestCase):
    def test_pending_signing_recovers_after_restart_without_body_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            db = f"{directory}/chat.sqlite"
            first_store = Store(db, "AG7SU")
            message = first_store.insert_local(
                "#test", "AG7SU", 1234, "already transmitted",
                signer="AG7SU")
            failed_message = first_store.insert_local(
                "#test", "AG7SU", 1235, "also already transmitted",
                signer="AG7SU")
            first_store.set_auth(failed_message["o"], failed_message["s"], None,
                                 "signing_failed", "AG7SU", "key unavailable")
            first_store.close()

            restarted_store = Store(db, "AG7SU")
            chat = Chat.__new__(Chat)
            chat.store = restarted_store
            chat.auth = SimpleNamespace(enabled=True)
            chat.auth_pending_lock = threading.RLock()
            chat.signing_jobs = set()
            chat.signing_retry_after = {}
            queued = []
            chat._queue_signature = lambda row: queued.append(dict(row)) or True

            chat._retry_pending_signatures()

            self.assertEqual([(row["o"], row["s"]) for row in queued],
                             [(message["o"], message["s"]),
                              (failed_message["o"], failed_message["s"])])
            self.assertEqual(queued[0]["b"], "already transmitted")
            self.assertEqual(queued[1]["b"], "also already transmitted")
            self.assertNotIn("proof", queued[0])
            # Recovery schedules only the detached proof job; this path has no
            # transport access and cannot replay the already-sent body.
            self.assertEqual(restarted_store.proof_for(message["o"], message["s"]), None)

            proof = b"durable detached proof"
            restarted_store.set_auth(message["o"], message["s"], proof,
                                     "pending", "AG7SU")
            restarted_store.close()

            after_sign_restart = Store(db, "AG7SU")
            remaining = after_sign_restart.pending_local_signatures(after_sign_restart.node)
            self.assertEqual([(row["o"], row["s"]) for row in remaining],
                             [(failed_message["o"], failed_message["s"])])
            self.assertEqual(after_sign_restart.proof_for(message["o"], message["s"]), proof)
            after_sign_restart.close()


if __name__ == "__main__":
    unittest.main()
