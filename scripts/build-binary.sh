#!/usr/bin/env sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_ROOT"

command -v uv >/dev/null 2>&1 || {
    echo "uv is required; install it from https://docs.astral.sh/uv/" >&2
    exit 1
}

uv sync --locked --extra bundle --no-dev
uv run --locked --extra bundle pyinstaller \
    --noconfirm --clean --onefile --name SUperChat \
    --paths "$PROJECT_ROOT" \
    --collect-all textual \
    --collect-all rich \
    --collect-all pygments \
    --collect-all PIL \
    --collect-data kisschat \
    scripts/kisschat_binary.py

echo "Built $PROJECT_ROOT/dist/SUperChat"
