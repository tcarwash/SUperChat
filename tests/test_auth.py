import json
import base64
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kisschat.auth import ARAPAuth, MEDIA_TYPE, CONTEXT


class ARAPVerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.keys = self.root / "keys"
        self.keys.mkdir()
        self.fingerprint = "ab" * 32
        (self.keys / f"{self.fingerprint}.pub.pem").write_text("test key")
        self.auth = ARAPAuth(key_dir=str(self.keys), trust_store="trust.json")
        self.message = {"o": "01" * 16, "s": 1, "ts": 100, "c": "#test",
                        "n": "N0CALL", "b": "hello"}
        self.inspected = {"authenticated": False,
                          "payload": {"4": "N0CALL"},
                          "protected_headers": {"4": "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s="}}
        # Replace the sample base64 value with the configured exact fingerprint.
        self.inspected["protected_headers"]["4"] = base64.b64encode(
            bytes.fromhex(self.fingerprint)).decode("ascii")

    def tearDown(self):
        self.temp.cleanup()

    def run_sequence(self, verify_json, verify_code=0):
        outputs = [subprocess.CompletedProcess([], 0, json.dumps(self.inspected).encode(), b""),
                   subprocess.CompletedProcess([], verify_code,
                                                json.dumps(verify_json).encode(), b"")]
        calls = []

        def fake_run(args, **kwargs):
            calls.append(args)
            return outputs.pop(0)

        return calls, patch.object(self.auth, "_run", side_effect=fake_run)

    def test_only_fully_accepted_structured_result_is_verified(self):
        result = {"cryptographic_signature": "valid", "content_integrity": "valid",
                  "callsign_identity": "trusted", "overall_policy": "accepted",
                  "claims": {"signer": "N0CALL", "key_id_sha256": self.fingerprint}}
        calls, mocked = self.run_sequence(result)
        with mocked:
            status, signer, _ = self.auth.verify(self.message, b"proof")
        self.assertEqual((status, signer), ("verified", "N0CALL"))
        self.assertTrue(any(arg.endswith(f"{self.fingerprint}.pub.pem") for arg in calls[1]))

    def test_valid_signature_without_trust_is_untrusted(self):
        result = {"cryptographic_signature": "valid", "content_integrity": "valid",
                  "callsign_identity": "untrusted", "overall_policy": "rejected",
                  "claims": {"signer": "N0CALL", "key_id_sha256": self.fingerprint}}
        _, mocked = self.run_sequence(result, verify_code=1)
        with mocked:
            status, _, _ = self.auth.verify(self.message, b"proof")
        self.assertEqual(status, "untrusted")

    def test_accepted_json_with_failing_cli_exit_is_not_verified(self):
        result = {"cryptographic_signature": "valid", "content_integrity": "valid",
                  "callsign_identity": "trusted", "overall_policy": "accepted",
                  "claims": {"signer": "N0CALL", "key_id_sha256": self.fingerprint}}
        _, mocked = self.run_sequence(result, verify_code=2)
        with mocked:
            status, _, _ = self.auth.verify(self.message, b"proof")
        self.assertEqual(status, "unverified")

    def test_incomplete_or_malformed_result_never_becomes_verified(self):
        result = {"cryptographic_signature": "valid", "content_integrity": "valid",
                  "callsign_identity": "trusted", "overall_policy": "accepted"}
        _, mocked = self.run_sequence(result)
        with mocked:
            status, _, _ = self.auth.verify(self.message, b"proof")
        self.assertEqual(status, "unverified")

    def test_verification_revision_changes_with_trust_policy(self):
        trust = self.root / "trust.json"
        auth = ARAPAuth(cli="missing-arap-test-command", key_dir=str(self.keys),
                        trust_store=str(trust))
        trust.write_text('{"version":1,"bindings":[]}')
        before = auth.verification_revision()
        trust.write_text('{"version":1,"bindings":[{"callsign":"N0CALL"}]}')
        self.assertNotEqual(before, auth.verification_revision())


if __name__ == "__main__":
    unittest.main()
