"""Optional ARAP CLI bridge for detached, end-to-end message signatures."""
from __future__ import annotations

import json
import hashlib
import os
import re
import subprocess
import tempfile
import base64
from pathlib import Path
from .images import MAX_EDGE
from .protocol import CHANNEL_NAME, MAX_MESSAGE_BYTES, MAX_THUMBNAIL_BYTES

MEDIA_TYPE = "application/vnd.kisschat.message+json"
CONTEXT = "kisschat/message-v1"
ARAP_CALLSIGN = re.compile(r"^(?=.{3,20}$)(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]+(?:/[A-Z0-9]+)*$")


class ARAPUnavailable(RuntimeError):
    """ARAP could not be run, so the signature has not been verified."""


def _inspect_claims(document: object) -> tuple[str, str]:
    """Extract untrusted signer and fingerprint from ARAP's inspection JSON."""
    if not isinstance(document, dict) or document.get("authenticated") is not False:
        raise ValueError("ARAP inspect-proof returned an unexpected response")
    payload = document.get("payload")
    headers = document.get("protected_headers")
    if not isinstance(payload, dict) or not isinstance(headers, dict):
        raise ValueError("ARAP inspect-proof omitted proof claims")
    signer = payload.get("4")
    fingerprint_b64 = headers.get("4")
    if not isinstance(signer, str) or not signer.isascii():
        raise ValueError("invalid ARAP signer claim")
    signer = signer.upper()
    if not ARAP_CALLSIGN.fullmatch(signer):
        raise ValueError("invalid ARAP signer claim")
    if not isinstance(fingerprint_b64, str):
        raise ValueError("invalid ARAP key fingerprint claim")
    try:
        fingerprint = base64.b64decode(fingerprint_b64, validate=True).hex()
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid ARAP key fingerprint claim") from exc
    if len(fingerprint) != 64:
        raise ValueError("invalid ARAP key fingerprint claim")
    return signer, fingerprint


class ARAPAuth:
    def __init__(self, *, cli: str = "arap", callsign: str = "", key: str = "",
                 key_dir: str = "", trust_store: str = "", passphrase_env: str = ""):
        self.cli, self.callsign, self.key = cli, callsign.upper(), key
        self.key_dir, self.trust_store = key_dir, trust_store
        self.passphrase_env = passphrase_env

    @property
    def enabled(self) -> bool:
        return bool(self.key and self.callsign)

    @staticmethod
    def content(message: dict, callsign: str) -> bytes:
        """Return the exact KISSChat-message-v1 bytes passed to ARAP."""
        callsign = callsign.upper()
        if not ARAP_CALLSIGN.fullmatch(callsign):
            raise ValueError("invalid KISSChat signing callsign")
        origin = message.get("o")
        if not isinstance(origin, str) or not re.fullmatch(r"[0-9a-f]{32}", origin):
            raise ValueError("invalid KISSChat origin UUID")
        sequence, timestamp = message.get("s"), message.get("ts")
        if type(sequence) is not int or not 1 <= sequence <= 0xFFFFFFFF:
            raise ValueError("KISSChat sequence must be an unsigned 32-bit nonzero integer")
        if type(timestamp) is not int or not 0 <= timestamp <= 0xFFFFFFFF:
            raise ValueError("KISSChat timestamp must be an unsigned 32-bit integer")
        channel, nick, body = message.get("c"), message.get("n"), message.get("b")
        if not isinstance(channel, str) or not CHANNEL_NAME.fullmatch(channel):
            raise ValueError("invalid KISSChat channel")
        if (not isinstance(nick, str) or not nick or len(nick.encode("utf-8")) > 16 or
                not all(char.isprintable() for char in nick)):
            raise ValueError("invalid KISSChat nick")
        if (not isinstance(body, str) or len(body.encode("utf-8")) > MAX_MESSAGE_BYTES or
                not all(char.isprintable() or char in "\t\n" for char in body)):
            raise ValueError("invalid KISSChat body")
        if not body and "image" not in message:
            raise ValueError("KISSChat message needs text or a thumbnail")
        content = {"body": body, "callsign": callsign, "channel": channel,
                   "nick": nick, "origin": origin, "sequence": sequence,
                   "timestamp": timestamp, "version": 1}
        if "image" in message:
            image = message["image"]
            if not isinstance(image, dict):
                raise ValueError("invalid KISSChat thumbnail metadata")
            digest, size = image.get("digest"), image.get("size")
            width, height = image.get("width"), image.get("height")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("invalid KISSChat thumbnail SHA-256")
            if type(size) is not int or not 1 <= size <= MAX_THUMBNAIL_BYTES:
                raise ValueError("invalid KISSChat thumbnail byte count")
            if (type(width) is not int or not 1 <= width <= MAX_EDGE or
                    type(height) is not int or not 1 <= height <= MAX_EDGE):
                raise ValueError("invalid KISSChat thumbnail dimensions")
            content["image"] = {"height": height, "media_type": "image/jpeg",
                                "sha256": digest, "size": size, "width": width}
        # Go's encoding/json (SetEscapeHTML(false)) always escapes these two
        # Unicode separators. Match it so the independently implemented ARAP
        # adapter can produce the exact same UTF-8 byte sequence.
        encoded = json.dumps(content, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"))
        encoded = encoded.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
        return encoded.encode("utf-8")

    def _run(self, args: list[str], *, stdin: bytes | None = None):
        try:
            return subprocess.run([self.cli, *args], input=stdin, capture_output=True,
                                  check=False, timeout=30)
        except (OSError, subprocess.SubprocessError) as exc:
            raise ARAPUnavailable(f"ARAP command unavailable: {exc}") from exc

    def verification_revision(self) -> str:
        """Fingerprint the local verifier, public-key candidates and trust policy."""
        digest = hashlib.sha256()
        digest.update(b"kisschat-auth-cache-v2\0")
        digest.update(f"{MEDIA_TYPE}\0{CONTEXT}\0KISSChat-message-v1\0identity-v1\0".encode())
        try:
            executable = Path(self.cli).resolve(strict=True)
            stat = executable.stat()
            digest.update(f"{executable}\0{stat.st_size}\0{stat.st_mtime_ns}\0".encode())
        except OSError as exc:
            digest.update(f"unavailable-cli:{type(exc).__name__}:{exc.errno}\0".encode())
        if self.trust_store:
            try:
                with open(self.trust_store, "rb") as source:
                    trust = source.read(1024 * 1024 + 1)
                if len(trust) > 1024 * 1024:
                    digest.update(b"trust-store-oversize\0")
                else:
                    digest.update(b"trust-store\0" + trust)
            except OSError as exc:
                digest.update(f"trust-store-error:{type(exc).__name__}:{exc.errno}\0".encode())
        else:
            digest.update(b"trust-store-unconfigured\0")
        if self.key_dir:
            directory = Path(self.key_dir)
            try:
                entries = sorted(directory.glob("*.pub.pem"))
                digest.update(f"key-count:{len(entries)}\0".encode())
                for key in entries[:4096]:
                    try:
                        stat = key.stat()
                        digest.update(f"{key.name}\0{stat.st_size}\0{stat.st_mtime_ns}\0".encode())
                    except OSError as exc:
                        digest.update(f"{key.name}\0error:{exc.errno}\0".encode())
            except OSError as exc:
                digest.update(f"key-dir-error:{type(exc).__name__}:{exc.errno}\0".encode())
        else:
            digest.update(b"key-dir-unconfigured\0")
        return digest.hexdigest()

    def sign(self, message: dict) -> bytes:
        if not self.enabled:
            return b""
        if not ARAP_CALLSIGN.fullmatch(self.callsign):
            raise ValueError("ARAP signatures require a 3–20 character callsign "
                             "using letters, digits, and internal slashes")
        with tempfile.TemporaryDirectory(prefix="kisschat-arap-") as directory:
            root = Path(directory)
            source, proof = root / "message.json", root / "message.cose"
            source.write_bytes(self.content(message, self.callsign))
            args = ["sign-content", "--key", self.key, "--callsign", self.callsign,
                    "--input", str(source), "--media-type", MEDIA_TYPE,
                    "--context", CONTEXT, "--canonicalization", "identity-v1",
                    "--out", str(proof)]
            stdin = None
            secret = os.environ.get(self.passphrase_env) if self.passphrase_env else None
            if secret is not None:
                args.append("--passphrase-stdin")
                stdin = secret.encode("utf-8") + b"\n"
            result = self._run(args, stdin=stdin)
            if result.returncode:
                raise RuntimeError(result.stderr.decode("utf-8", "replace").strip() or
                                   f"ARAP signing exited {result.returncode}")
            return proof.read_bytes()

    def verify(self, message: dict, proof: bytes) -> tuple[str, str, str]:
        """Return status, signer, detail. Only ARAP policy acceptance is verified."""
        key_dir = Path(self.key_dir) if self.key_dir else None
        with tempfile.TemporaryDirectory(prefix="kisschat-arap-") as directory:
            root = Path(directory)
            proof_path, content_path = root / "message.cose", root / "message.json"
            proof_path.write_bytes(proof)
            try:
                inspect = self._run(["inspect-proof", "--proof", str(proof_path)])
                if inspect.returncode != 0:
                    detail = inspect.stderr.decode("utf-8", "replace").strip()
                    return "invalid", "", detail or "ARAP rejected the proof envelope"
                signer, fingerprint = _inspect_claims(json.loads(inspect.stdout))
            except ARAPUnavailable as exc:
                return "unverified", "", str(exc)
            except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                return "invalid", "", f"Malformed ARAP proof: {exc}"
            if not key_dir or not key_dir.is_dir():
                return "unverified", signer, "No ARAP public-key directory configured"
            # Fingerprint-addressed files allow key rotation without replacing
            # historical public keys. Keep the callsign filename as a migration
            # fallback for existing local setups.
            public_key = key_dir / f"{fingerprint}.pub.pem"
            if not public_key.is_file():
                public_key = key_dir / f"{signer.replace('/', '-')}.pub.pem"
            if not public_key.is_file():
                return "unverified", signer, f"No local public key for {signer} ({fingerprint})"
            content_path.write_bytes(self.content(message, signer))
            args = ["verify-content", "--proof", str(proof_path), "--input", str(content_path),
                    "--public-key", str(public_key), "--trust-store", self.trust_store or "",
                    "--expect-media-type", MEDIA_TYPE, "--expect-context", CONTEXT,
                    "--expect-canonicalization", "identity-v1",
                    "--expect-callsign", signer]
            try:
                result = self._run(args)
            except ARAPUnavailable as exc:
                return "unverified", signer, str(exc)
            try:
                verification = json.loads(result.stdout)
            except (json.JSONDecodeError, UnicodeDecodeError):
                verification = None
            stderr = result.stderr.decode("utf-8", "replace").strip()
            if not isinstance(verification, dict):
                return "unverified", signer, stderr or "ARAP returned no structured verification result"
            detail = str(verification.get("reason") or stderr or
                         f"ARAP policy: {verification.get('overall_policy', 'unknown')}")
            signature = verification.get("cryptographic_signature")
            integrity = verification.get("content_integrity")
            identity = verification.get("callsign_identity")
            policy = verification.get("overall_policy")
            verified_claims = verification.get("claims")
            verified_signer = (verified_claims.get("signer")
                               if isinstance(verified_claims, dict) else None)
            if (signature == "valid" and integrity == "valid" and
                    identity == "trusted" and policy == "accepted" and
                    isinstance(verified_signer, str) and
                    verified_signer.upper() == signer and
                    verified_claims.get("key_id_sha256") == fingerprint):
                return "verified", signer, detail
            if signature == "invalid" or integrity == "invalid":
                return "invalid", signer, detail
            if signature == "valid" and integrity == "valid" and identity == "untrusted":
                return "untrusted", signer, detail
            if signature == "valid" and integrity == "valid" and identity == "rejected":
                return "invalid", signer, detail
            return "unverified", signer, detail
