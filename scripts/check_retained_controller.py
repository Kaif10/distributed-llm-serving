"""Check single-member controller retention over real HTTP, files, and process kills.

Two unique configuration updates plus logged historical retries, not inference traffic.
The default crosses the original absolute 1024-entry ceiling without dropping retry state.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def request(port, method, path, payload=None, raw=None):
    data = raw if raw is not None else (json.dumps(payload).encode() if payload is not None else None)
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=7) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        try:
            return error.code, error.read().decode("utf-8")
        finally:
            error.close()


def ports():
    with socket.socket() as peer, socket.socket() as client:
        peer.bind(("127.0.0.1", 0))
        client.bind(("127.0.0.1", 0))
        return peer.getsockname()[1], client.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--go", default="go")
    parser.add_argument("--retries", type=int, default=1100)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not 1 <= args.retries <= 10000:
        parser.error("--retries must be 1..10000")
    output, binary = args.output.resolve(), args.binary.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="retained-", dir=output.parent)).resolve()
    if root.parent != output.parent or not root.name.startswith("retained-"):
        raise RuntimeError("unexpected experiment directory; refusing cleanup")
    storage = root / "store"
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "go_version": subprocess.check_output([args.go, "version", str(binary)], text=True).strip(),
              "command": [sys.executable, *sys.argv], "platform": platform.platform(),
              "scope": "single-member controller, real loopback HTTP/synced files/process kills; no inference",
              "unique_updates": 2, "requested_historical_retries": args.retries,
              "completed_historical_retries": 0, "commands": [], "retentions": [], "restarts": [],
              "experiment_directory": str(root), "outcome": "failed"}
    process, log = None, None
    port = None

    def start():
        nonlocal process, log, port
        peer, port = ports()
        command = [str(binary), "-id", "a", "-peers", f"a=http://127.0.0.1:{peer}",
                   "-client-listen", f"127.0.0.1:{port}", "-configuration",
                   "-retained-dir", str(storage), "-retained-group", "retention-check"]
        result["commands"].append(command)
        log = (root / "process.log").open("ab")
        process = subprocess.Popen(command, stdout=log, stderr=log)
        deadline = time.monotonic()+12
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("controller exited: " + (root / "process.log").read_text(encoding="utf-8"))
            try:
                code, state = request(port, "GET", "/config")
                if code == 200:
                    return state
            except (OSError, TimeoutError):
                pass
            time.sleep(0.025)
        raise RuntimeError("controller did not serve a fresh read")

    def kill():
        nonlocal process, log
        if process is not None:
            if process.poll() is None:
                process.kill()  # Only the child created by this runner.
            process.wait(timeout=10)
            process = None
        if log is not None:
            log.close()
            log = None

    def status():
        code, state = request(port, "GET", "/status")
        assert code == 200, (code, state)
        return state

    def retain():
        code, capture = request(port, "POST", "/retain")
        assert code == 200, (code, capture)
        state = status()
        assert state["log"] == [] and state["applied_index"] == state["commit_index"] == capture["base_index"]
        assert state["retention"] == capture and capture["base_index"] > 0
        code, again = request(port, "POST", "/retain")
        assert code == 200 and again == capture, "same-prefix maintenance must be idempotent"
        sizes = {path.name: path.stat().st_size for path in storage.iterdir()}
        assert set(sizes) == {"owner.wal", "image-0.wal", "image-1.wal"}
        assert sum(sizes.values()) < 49*(1 << 20), "retained store exceeded logical file bound"
        result["retentions"].append({**capture, "file_bytes": sizes})
        return capture

    try:
        assert start() == {"revision": 0, "workers": []}
        first = {"request_id": "drain", "expected_revision": 0,
                 "workers": [{"id": "worker-a", "origin": "http://127.0.0.1:20001", "draining": True}]}
        second = {"request_id": "resume", "expected_revision": 1, "workers": []}
        code, original = request(port, "PUT", "/config", first)
        assert code == 200 and original["revision"] == 1
        code, current = request(port, "PUT", "/config", second)
        assert code == 200 and current == {"revision": 2, "workers": []}
        code, _ = request(port, "POST", "/retain", raw=b"{}")
        assert code == 400
        last = retain()
        for i in range(args.retries):
            code, historical = request(port, "PUT", "/config", first)
            assert code == 200 and historical == original, (i, code, historical)
            result["completed_historical_retries"] += 1
            if (i+1) % 64 == 0:
                last = retain()
        before = status()
        result["before_kill"] = before
        assert before["configuration"] == current
        assert before["commit_index"] == args.retries+3
        kill()
        assert start() == current
        restored = status()
        assert restored["retention"]["recovered_index"] == last["base_index"]
        assert restored["retention"]["sha256"] == last["sha256"]
        assert restored["applied_index"] == args.retries+4
        result["restarts"].append(restored)
        code, historical = request(port, "PUT", "/config", first)
        assert code == 200 and historical == original
        assert request(port, "GET", "/config") == (200, current)
        last = retain()
        kill()
        assert start() == current
        restored = status()
        assert restored["retention"]["recovered_index"] == last["base_index"]
        assert restored["retention"]["sha256"] == last["sha256"]
        assert request(port, "PUT", "/config", first) == (200, original)
        result["restarts"].append(restored)
        result["original_retry_result"], result["current_configuration"] = original, current
        result["crossed_original_absolute_entry_ceiling"] = before["commit_index"] > 1024
        result["outcome"] = "passed"
    except Exception as error:
        result["error"] = repr(error)
        raise
    finally:
        kill()
        output.write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")
        if result["outcome"] == "passed":
            shutil.rmtree(root)
    print(json.dumps({"outcome": result["outcome"], "historical_retries": args.retries,
                      "absolute_commit_index_before_kill": result["before_kill"]["commit_index"],
                      "crossed_original_absolute_entry_ceiling": result["crossed_original_absolute_entry_ceiling"],
                      "output": str(output)}))


if __name__ == "__main__":
    main()
