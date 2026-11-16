import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quest", type=int, choices=range(1, 8))
    parser.add_argument("--extra", action="store_true")
    args = parser.parse_args()
    cases = json.loads((ROOT / "verification_cases.json").read_text(encoding="utf-8"))
    failures = 0
    environment = {**os.environ, "PYTHONIOENCODING": "utf-8",
                   "PYSPARK_PYTHON": sys.executable}
    for number, module, options, expected in cases:
        if args.quest is not None and args.quest != number:
            continue
        if any("data/check/" in value for value in options) and not args.extra:
            continue
        command = [sys.executable, "-m", module, *options]
        label = f"Q{number:02d} {' '.join(options)}".rstrip()
        try:
            run = subprocess.run(command, cwd=ROOT, env=environment,
                                 capture_output=True, text=True, encoding="utf-8",
                                 errors="replace", timeout=180)
            if run.returncode:
                raise RuntimeError(run.stderr[-5000:] or run.stdout[-5000:])
            records = [line.removeprefix("RESULT ") for line in run.stdout.splitlines()
                       if line.startswith("RESULT ")]
            if len(records) != 1 or json.loads(records[0]) != expected:
                raise ValueError(f"expected {expected!r}, received {records!r}")
            if number == 3 and "CHECKPOINT 1000 EXACTLY_ONCE" not in run.stdout.splitlines():
                raise ValueError("Checkpoint settings do not match")
        except (RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            print(f"FAIL {label}\n{error}", flush=True)
            failures += 1
        else:
            print(f"PASS {label}", flush=True)
    return 1 if failures else 0

if __name__ == "__main__":
    sys.exit(main())
