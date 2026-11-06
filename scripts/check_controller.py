"""Exercise the real controller executable, local journal, HTTP CAS, and crash recovery."""

import concurrent.futures
import contextlib
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import urllib.error
import urllib.request

from check_real_inference import ROOT, get_json
from check_real_pool import service

ORIGIN = "http://127.0.0.1:19090"


def put(update):
    request = urllib.request.Request(ORIGIN+"/config", data=json.dumps(update).encode(),
                                     headers={"Content-Type": "application/json"}, method="PUT")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as err:
        code, text = err.code, err.read().decode()
        err.close()
        return code, text


def update(key, revision, draining):
    return {"request_id": key, "expected_revision": revision,
            "workers": [{"id": "worker-a", "origin": "http://127.0.0.1:18081", "draining": draining}]}


def main():
    output = ROOT/"artifacts/controller-check"
    output.mkdir(parents=True, exist_ok=True)
    for port in [19090, 19091]:
        with socket.socket() as probe: probe.bind(("127.0.0.1", port))
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
              "observations": []}
    try:
        with tempfile.TemporaryDirectory(prefix="journal-", dir=output) as temporary:
            journal = Path(temporary)/"fleet.wal"
            command = [str(ROOT/"bin/controller.exe"), "-listen", "127.0.0.1:19090", "-journal", str(journal)]
            with service(command, ORIGIN+"/config", output/"initial.log") as process:
                assert get_json(ORIGIN, "/config")["revision"] == 0
                status, state = put(update("drain", 0, True)); assert status == 200 and state["revision"] == 1
                second = subprocess.run([command[0], "-listen", "127.0.0.1:19091", "-journal", str(journal)],
                                        capture_output=True, text=True, timeout=5)
                assert second.returncode != 0 and "exclusive journal lock" in second.stderr
                result["observations"].append({"acknowledged_before_kill": state, "second_writer_rejected": True})
                process.kill(); process.wait(timeout=5)
            with service(command, ORIGIN+"/config", output/"recovered.log") as process:
                state = get_json(ORIGIN, "/config"); assert state["revision"] == 1 and state["workers"][0]["draining"]
                assert put(update("drain", 0, True))[0] == 200
                assert put(update("resume", 1, False))[1]["revision"] == 2
                status, old = put(update("drain", 0, True)); assert status == 200 and old["revision"] == 1
                assert get_json(ORIGIN, "/config")["revision"] == 2
                assert put(update("stale", 0, False))[0] == 409
                assert put(update("drain", 0, False))[0] == 409
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as writers:
                    statuses = list(writers.map(lambda i: put(update("writer-"+str(i), 2, True))[0], range(8)))
                assert statuses.count(200) == 1 and statuses.count(409) == 7
                result["observations"].append({"recovered_revision": 1, "old_retry_result": old,
                                              "concurrent_writer_statuses": statuses})
                process.kill(); process.wait(timeout=5)
            # Simulate an interrupted next append, beyond the acknowledged revision 3.
            with journal.open("ab") as target: target.write(b"\x10\x00\x00")
            with service(command, ORIGIN+"/config", output/"tail-recovery.log") as process:
                assert get_json(ORIGIN, "/config")["revision"] == 3
                status, state = put(update("after-tail", 3, False)); assert status == 200 and state["revision"] == 4
                result["observations"].append({"acknowledged_after_tail_recovery": state})
                process.kill(); process.wait(timeout=5)
            with service(command, ORIGIN+"/config", output/"final.log"):
                state = get_json(ORIGIN, "/config"); assert state["revision"] == 4 and not state["workers"][0]["draining"]
                result["final"] = state
        result["passed"] = True
        print("PASS: acknowledged state survives process kills; CAS, retries, writer exclusion, and incomplete-tail recovery hold", flush=True)
    except Exception as err:
        result.update(passed=False, error=str(err)); raise
    finally:
        (output/"results.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
