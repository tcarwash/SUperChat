#!/usr/bin/env python3
"""Run the real three-station simulator with disposable, locally trusted ARAP keys."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

CALLSIGNS = ("K0AAA", "K0BBB", "K0CCC")
TEST_PASSPHRASE = "KISSChat simulator test key only"


def run(command: list[str], *, input_data: bytes | None = None) -> bytes:
    result = subprocess.run(command, input=input_data, capture_output=True, check=False)
    if result.returncode:
        detail = result.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(detail or f"command failed: {command[0]}")
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_cli = Path(__file__).resolve().parents[2] / "arap" / "bin" / "arap"
    parser.add_argument("--arap-cli", type=Path, default=default_cli,
                        help="built ARAP CLI (default: sibling ../arap/bin/arap)")
    parser.add_argument("simulator_args", nargs=argparse.REMAINDER,
                        help="arguments forwarded to simulate_mesh.py; use -- before them")
    opts = parser.parse_args()
    cli = opts.arap_cli.resolve(strict=True)
    if not opts.simulator_args:
        parser.error("provide simulator arguments after --, including --script and --json-report")
    forwarded = opts.simulator_args[1:] if opts.simulator_args[0] == "--" else opts.simulator_args

    with tempfile.TemporaryDirectory(prefix="kisschat-signed-mesh-") as temporary:
        root = Path(temporary)
        private = root / "private"
        public = root / "public"
        private.mkdir()
        public.mkdir()
        trust = root / "trust.json"
        secret = (TEST_PASSPHRASE + "\n").encode("utf-8")

        for callsign in CALLSIGNS:
            private_key = private / f"{callsign}.age"
            public_key = root / f"{callsign}.pub.pem"
            run([str(cli), "keygen", "--private-key", str(private_key),
                 "--public-key", str(public_key), "--passphrase-stdin"],
                input_data=secret + secret)
            fingerprint_output = run([str(cli), "fingerprint", "--public-key",
                                      str(public_key)]).decode("utf-8", "replace").strip()
            fingerprint = fingerprint_output.split()[-1].lower()
            if len(fingerprint) != 64 or any(ch not in "0123456789abcdef"
                                             for ch in fingerprint):
                raise RuntimeError(f"ARAP returned an invalid fingerprint for {callsign}")
            shutil.copyfile(public_key, public / f"{fingerprint}.pub.pem")
            run([str(cli), "trust", "add", "--store", str(trust), "--public-key",
                 str(public_key), "--callsign", callsign])

        env = os.environ.copy()
        env["KISSCHAT_SIM_ARAP_DIR"] = str(root)
        env["KISSCHAT_SIM_ARAP_CLI"] = "arap"
        env["KISSCHAT_ARAP_PASSPHRASE"] = TEST_PASSPHRASE
        env["PATH"] = str(cli.parent) + os.pathsep + env.get("PATH", "")
        simulator = Path(__file__).with_name("simulate_mesh.py")
        return subprocess.run([sys.executable, str(simulator), *forwarded], env=env).returncode


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError) as exc:
        print(f"signed mesh setup failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
