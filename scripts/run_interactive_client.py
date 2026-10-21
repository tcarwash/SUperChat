#!/usr/bin/env python3
"""Record this terminal child PID, then replace it with the client command."""
from __future__ import annotations

import os
from pathlib import Path
import sys


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("usage: run_interactive_client.py PID_FILE COMMAND [ARG ...]")
    pid_file = Path(sys.argv[1])
    command = sys.argv[2:]
    pid_file.write_text(f"{os.getpid()}\n", encoding="ascii")
    try:
        os.execvp(command[0], command)
    except BaseException:
        pid_file.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    main()
