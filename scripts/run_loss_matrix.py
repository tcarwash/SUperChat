#!/usr/bin/env python3
"""Run one deterministic three-station scenario across a packet-loss matrix."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--script", type=Path, default=Path("fixtures/loss_matrix_smoke.txt"))
    parser.add_argument("--output", type=Path,
                        default=Path("loss-matrix-report.json"))
    parser.add_argument("--seed", type=int, default=7,
                        help="same seed at each loss rate for reproducible comparisons")
    parser.add_argument("--loss", type=float, nargs="+", default=[0, 0.05, 0.15, 0.30],
                        help="packet-loss rates to run (default: 0 0.05 0.15 0.30)")
    parser.add_argument("--loss-model", choices=("independent", "bursty"),
                        default="independent")
    parser.add_argument("--burst-recovery", type=float, default=0.5)
    parser.add_argument("--retry-seconds", type=float, default=2.0,
                        help="simulator sync retry interval (default 2 seconds)")
    parser.add_argument("--time-scale", type=float, default=0.1)
    opts = parser.parse_args()
    script = opts.script.resolve()
    if not script.is_file():
        parser.error(f"scenario file does not exist: {script}")
    if (not opts.loss or any(not 0 <= rate <= 1 for rate in opts.loss) or
            opts.retry_seconds <= 0 or not 0 < opts.time_scale <= 10 or
            not 0 < opts.burst_recovery <= 1):
        parser.error("loss rates must be in 0..1, retry must be positive, and time-scale in (0, 10]")

    simulator = Path(__file__).with_name("simulate_mesh.py").resolve()
    runs = []
    with tempfile.TemporaryDirectory(prefix="kisschat-loss-matrix-") as directory:
        root = Path(directory)
        for index, rate in enumerate(opts.loss):
            report_path = root / f"run-{index}-loss-{rate:.3f}.json"
            command = [sys.executable, str(simulator), "--topology", "line",
                       "--loss", str(rate), "--seed", str(opts.seed),
                       "--loss-model", opts.loss_model,
                       "--burst-recovery", str(opts.burst_recovery),
                       "--contention-window", "10", "--time-scale", str(opts.time_scale),
                       "--sync-retry-seconds", str(opts.retry_seconds),
                       "--trace-sync", "--script", str(script),
                       "--json-report", str(report_path)]
            result = subprocess.run(command, text=True, capture_output=True, check=False)
            if report_path.exists():
                report = json.loads(report_path.read_text(encoding="utf-8"))
            else:
                report = {"passed": False, "error": "simulator did not write its JSON report"}
            runs.append({"loss": rate, "loss_model": opts.loss_model,
                         "burst_recovery": opts.burst_recovery, "seed": opts.seed,
                         "exit_code": result.returncode, "report": report,
                         "stdout": result.stdout, "stderr": result.stderr})
            print(f"loss={rate:.0%} passed={report.get('passed', False)} "
                  f"exit={result.returncode}", flush=True)

    passed = all(run["exit_code"] == 0 and run["report"].get("passed") is True
                 for run in runs)
    opts.output.parent.mkdir(parents=True, exist_ok=True)
    opts.output.write_text(json.dumps({"schema": 1, "passed": passed,
                                       "script": str(script), "seed": opts.seed,
                                       "loss_model": opts.loss_model,
                                       "burst_recovery": opts.burst_recovery,
                                       "runs": runs}, indent=2, sort_keys=True) + "\n",
                           encoding="utf-8")
    print(f"matrix {'PASS' if passed else 'FAIL'}: {opts.output}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
