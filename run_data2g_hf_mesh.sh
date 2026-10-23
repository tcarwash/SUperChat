#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

run_mesh() {
  uv run python scripts/simulate_mesh.py --transport broadcast --topology line \
    --loss 0.05 --channel-busy-delay 5 --time-scale 0.1
}

if [[ -n "${TMUX:-}" ]]; then
  run_mesh
else
  exec tmux new-session -A -s data2g-hf-mesh \
    "bash -lc 'cd $(printf '%q' "$PWD") && uv run python scripts/simulate_mesh.py --transport broadcast --topology line --loss 0.05 --channel-busy-delay 5 --time-scale 0.1'"
fi
