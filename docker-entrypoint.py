#!/usr/local/bin/python
"""Repair the persistent volume owner, then run KISSChat without root."""
from __future__ import annotations

import os
import pwd
import sys


DATA_DIR = "/home/kisschat/.local/share/kisschat"
USER_NAME = "kisschat"


def _chown_volume(uid: int, gid: int) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    for directory, children, files in os.walk(DATA_DIR, followlinks=False):
        for name in (*children, *files):
            path = os.path.join(directory, name)
            if not os.path.islink(path):
                os.chown(path, uid, gid)
        os.chown(directory, uid, gid)


def main() -> None:
    if os.geteuid() == 0:
        account = pwd.getpwnam(USER_NAME)
        _chown_volume(account.pw_uid, account.pw_gid)
        os.setgroups([])
        os.setgid(account.pw_gid)
        os.setuid(account.pw_uid)
    os.execvp("kisschat", ["kisschat", *sys.argv[1:]])


if __name__ == "__main__":
    main()
