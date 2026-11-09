"""Three real peers: offline follower snapshot catch-up, leader loss, and full restart."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import tempfile
import time

from check_retained_controller import ports, request

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    binary, output = args.binary.resolve(), args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    pairs = {node: ports() for node in "abc"}
    members = ",".join(f"{node}=http://127.0.0.1:{pairs[node][0]}" for node in "abc")
    processes, logs = {}, []
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "platform": platform.platform(),
              "scope": "three native processes on one host; real HTTP/files; no inference or multi-host claim",
              "commands": [], "events": [], "passed": False}

    def call(node, method="GET", path="/status", payload=None, raw=None):
        return request(pairs[node][1], method, path, payload, raw)

    def wait(check, label):
        deadline = time.monotonic()+18
        while time.monotonic() < deadline:
            try:
                value = check()
                if value:
                    return value
            except (OSError, ValueError, KeyError):
                pass
            time.sleep(.025)
        raise RuntimeError("timeout: "+label)

    def leader():
        for node, process in processes.items():
            if process.poll() is None:
                code, state = call(node)
                if code == 200 and state["node"]["Role"] == "leader" and call(node, path="/config")[0] == 200:
                    return node

    def stop(node):
        if node in processes:
            process = processes.pop(node)
            if process.poll() is None:
                process.kill()
            process.wait(timeout=8)

    try:
        with tempfile.TemporaryDirectory(prefix="replicated-retention-") as temporary:
            def start(node):
                command = [str(binary), "-configuration", "-id", node, "-peers", members,
                           "-client-listen", f"127.0.0.1:{pairs[node][1]}",
                           "-retained-dir", str(Path(temporary)/node), "-retained-group", "replicated-check"]
                result["commands"].append(command)
                log = (output.parent/f"retained-peer-{node}.log").open("ab")
                logs.append(log)
                processes[node] = subprocess.Popen(command, stdout=log, stderr=log)
                wait(lambda: call(node)[0] == 200, "peer listener "+node)

            try:
                for node in "abc":
                    start(node)
                initial = wait(leader, "initial leader")
                first = {"request_id": "first", "expected_revision": 0,
                         "workers": [{"id": "worker", "origin": "http://127.0.0.1:20001", "draining": True}]}
                code, original = call(initial, "PUT", "/config", first)
                assert code == 200 and original["revision"] == 1
                lagger = next(node for node in "abc" if node != initial)
                wait(lambda: call(lagger)[1]["configuration"]["revision"] == 1, "lagger initial state")
                stop(lagger)
                for revision in range(1, 5):
                    code, state = call(initial, "PUT", "/config", {"request_id": f"update-{revision}", "expected_revision": revision, "workers": []})
                    assert code == 200 and state["revision"] == revision+1
                code, capture = call(initial, "POST", "/retain")
                assert code == 200 and capture["base_index"] > 2
                result["events"].append({"retained_leader": initial, "offline_peer": lagger, "capture": capture})
                code, state = call(initial, "PUT", "/config", {"request_id": "suffix", "expected_revision": 5, "workers": []})
                assert code == 200 and state["revision"] == 6
                start(lagger)
                def installed():
                    code, state = call(lagger)
                    return state if code == 200 and state["retention"]["installed_index"] >= capture["base_index"] and state["configuration"]["revision"] == 6 else None
                result["installed_peer"] = wait(installed, "snapshot and suffix applied by lagging peer")
                # Group checks occur before Step: a wrong-group message cannot advance term.
                before = call(initial)[1]["node"]["Term"]
                code, _ = request(pairs[initial][0], "POST", "/raft/message", {"Type": "read_probe", "From": lagger,
                                  "To": initial, "Term": before+100, "ReadID": 1, "Group": "wrong-group"})
                assert code == 400 and call(initial)[1]["node"]["Term"] == before
                stop(initial)
                replacement = wait(leader, "surviving majority leader")
                assert call(replacement, "PUT", "/config", first) == (200, original)
                assert call(replacement, path="/config") == (200, {"revision": 6, "workers": []})
                result["events"].append({"killed_leader": initial, "replacement": replacement})
                # Capture the current majority's known committed image before full restart.
                assert call(replacement, "POST", "/retain")[0] == 200
                start(initial)
                wait(lambda: call(initial)[1]["configuration"]["revision"] == 6, "old leader catches up")
                for node in list(processes):
                    stop(node)
                for node in "abc":
                    start(node)
                restarted = wait(leader, "leader after full restart")
                assert call(restarted, path="/config") == (200, {"revision": 6, "workers": []})
                assert call(restarted, "PUT", "/config", first) == (200, original)
                for node in "abc":
                    wait(lambda n=node: call(n)[1]["configuration"]["revision"] == 6, "restarted application "+node)
                result["after_full_restart"] = {node: call(node)[1] for node in "abc"}
                result["original_retry_result"] = original
                result["passed"] = True
                print("PASS: lagging-peer snapshot/suffix, durable retry history, leader loss, wrong-group rejection, and full-cluster restart", flush=True)
            finally:
                for node in list(processes):
                    stop(node)
                for log in logs:
                    log.close()
    except Exception as error:
        result["error"] = repr(error)
        raise
    finally:
        output.write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
