"""Record actual child-process kills at checkpoint publication boundaries.

The Go fault controls exist only in the test executable. No serving journal is changed.
"""
import argparse
import json
from pathlib import Path
import platform
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default="go", help="Go executable")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/checkpoint-publication/run.json")
    args = parser.parse_args()
    command = [args.go, "test", "-json", "./internal/checkpoint", "-run", "TestPublicationProcess", "-count=1"]
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
              "go_version": subprocess.check_output([args.go, "version"], text=True).strip(),
              "platform": platform.platform(), "python": platform.python_version(),
              "command": command, "invocation": [sys.executable, *sys.argv],
              "scope": "real test subprocesses killed at write/sync/ack boundaries; actual files, no Raft installation or inference"}
    try:
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=120)
        result.update(returncode=completed.returncode, stderr=completed.stderr,
                      events=[json.loads(line) for line in completed.stdout.splitlines() if line.strip()])
        rows = []
        for event in result["events"]:
            match = re.search(r"stage=(\S+) selected_generation=(\d+) retry_generation=(\d+) acknowledged_before_kill=(true|false)", event.get("Output", ""))
            if match:
                rows.append({"stage": match[1], "selected_generation": int(match[2]),
                             "retry_generation": int(match[3]), "acknowledged_before_kill": match[4] == "true"})
        result["crash_outcomes"] = rows
        expected = {"partial": 1, "before-sync": 2, "after-sync": 2, "acknowledged": 2}
        assert completed.returncode == 0, completed.stderr + completed.stdout
        assert {row["stage"]: row["selected_generation"] for row in rows} == expected and len(rows) == 4, rows
        assert all(row["retry_generation"] == 2 for row in rows), rows
        assert any(e.get("Action") == "pass" and e.get("Test") == "TestPublicationProcessExclusiveOwner" for e in result["events"]), "missing process lock check"
        result["passed"] = True
        print("PASS: partial/complete uncertain/acknowledged process-kill recovery, idempotent retry, and exclusive ownership")
    except Exception as error:
        result.update(passed=False, error=str(error))
        raise
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
