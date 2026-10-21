import json
import hashlib
import unittest
from pathlib import Path

from kisschat.auth import ARAPAuth


VECTOR_FILE = (Path(__file__).resolve().parents[2] / "arap" / "test-vectors" /
               "kisschat-message-v1" / "manifest.json")


class KISSChatProfileTests(unittest.TestCase):
    def test_shared_golden_vectors(self):
        manifest = json.loads(VECTOR_FILE.read_text(encoding="utf-8"))
        for vector in manifest["vectors"]:
            with self.subTest(vector=vector["name"]):
                actual = ARAPAuth.content(vector["message"], vector["signing_callsign"])
                self.assertEqual(actual.decode("utf-8"), vector["canonical"])

    def test_rejects_invalid_profile_values(self):
        base = {"o": "0123456789abcdef0123456789abcdef", "s": 1, "ts": 1,
                "c": "#test", "n": "AG7SU", "b": "hello"}
        cases = [
            {"s": 0}, {"s": True}, {"ts": 2**32}, {"o": "AG7SU"},
            {"c": "test"}, {"n": "x" * 17}, {"b": "x" * 4097},
            {"b": ""},
            {"image": None},
            {"image": {"digest": "A" * 64, "size": 1, "width": 1, "height": 1}},
        ]
        for change in cases:
            with self.subTest(change=change), self.assertRaises(ValueError):
                ARAPAuth.content({**base, **change}, "AG7SU")

    def test_accepts_maximum_body_and_wire_integer_values(self):
        message = {"o": "f" * 32, "s": 0xFFFFFFFF, "ts": 0xFFFFFFFF,
                   "c": "#max", "n": "AG7SU", "b": "x" * 4096}
        canonical = ARAPAuth.content(message, "AG7SU")
        self.assertEqual(hashlib.sha256(canonical).hexdigest(),
                         "51a6f895db1df564aae0ba7ed0858f1b2b35a8616a2cdb1eb09c62216a33cf6e")


if __name__ == "__main__":
    unittest.main()
