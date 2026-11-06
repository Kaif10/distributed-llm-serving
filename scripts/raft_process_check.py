"""Exercise three real raftpeer processes and independent journals on loopback.

No model traffic: these are opaque Raft commands, not controller API updates.
Usage: python scripts/raft_process_check.py bin/raftpeer.exe --output artifacts/raft-process/run.json
"""
import argparse
import base64
import concurrent.futures
import hashlib
import json
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def request(port, path, data=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data)
    try:
        with urllib.request.urlopen(req, timeout=7) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    # Hold all reservations together to obtain six distinct ports. Closing before
    # process bind leaves a local port-allocation race; startup errors fail the run.
    reservations = [socket.socket() for _ in range(6)]
    for sock in reservations:
        sock.bind(("127.0.0.1", 0))
    ports = [sock.getsockname()[1] for sock in reservations]
    for sock in reservations:
        sock.close()
    ids = ["a", "b", "c"]
    peers = ",".join(f"{node}=http://127.0.0.1:{ports[i]}" for i, node in enumerate(ids))
    processes, logs, acknowledged, events = {}, [], [], []
    report = {"platform": platform.platform(), "python": platform.python_version(),
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "working_tree": subprocess.check_output(["git", "status", "--porcelain"], text=True),
              "command": [sys.executable, *sys.argv],
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "scope": "three OS processes, same-host loopback, opaque commands, no inference",
              "events": events}
    with tempfile.TemporaryDirectory(prefix="fleet-raft-") as directory:
        root = Path(directory)

        def start(node):
            i = ids.index(node)
            log = open(root / f"{node}.log", "ab")
            logs.append(log)
            processes[node] = subprocess.Popen(
                [str(binary), "-id", node, "-peers", peers, "-client-listen",
                 f"127.0.0.1:{ports[i+3]}", "-journal", str(root / f"{node}.wal")],
                stdout=log, stderr=log)

        def stop(node):
            proc = processes.pop(node)
            proc.kill()
            proc.wait(timeout=5)

        def status(node):
            code, body = request(ports[ids.index(node)+3], "/status")
            if code != 200:
                raise RuntimeError(f"status {node}: {code} {body}")
            return json.loads(body)

        def wait_for(operation, label):
            deadline = time.monotonic() + 15
            last = None
            while time.monotonic() < deadline:
                for node, proc in processes.items():
                    if proc.poll() is not None:
                        raise RuntimeError(f"peer {node} exited: {(root / (node+'.log')).read_text(encoding='utf-8')}")
                try:
                    value = operation()
                    if value:
                        return value
                except (OSError, ValueError, RuntimeError) as error:
                    last = str(error)
                time.sleep(.05)
            raise AssertionError(f"timeout: {label}; last={last}")

        def leader():
            leaders = [node for node in processes if status(node)["node"]["Role"] == "leader"]
            return leaders[0] if len(leaders) == 1 else None

        def propose(node, data):
            code, body = request(ports[ids.index(node)+3], "/propose", data)
            if code == 200:
                ack = json.loads(body)
                assert ack["committed"]
                acknowledged.append({**ack, "data": base64.b64encode(data).decode("ascii")})
            return {"node": node, "status": code, "body": body}

        def converged():
            states = {node: status(node) for node in processes}
            first = next(iter(states.values()))
            if any(s["log"] != first["log"] or s["commit_index"] != len(s["log"] or []) for s in states.values()):
                return None
            for ack in acknowledged:
                for state in states.values():
                    entry = state["log"][ack["index"]-1]
                    assert entry["term"] == ack["term"] and entry.get("data") == ack["data"], "lost acknowledged command"
            return states

        try:
            for node in ids:
                start(node)
            first = wait_for(leader, "initial leader")
            for i in range(4):
                result = propose(first, f"before-failure-{i}".encode())
                assert result["status"] == 200, result
            events.append({"initial_leader": first, "initial_commits": 4})
            # Kill the leader while a real client request is in flight. This
            # request may commit before the kill, fail HTTP, or have unknown outcome.
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(propose, first, b"during-leader-kill")
                time.sleep(.002)
                stop(first)
                try:
                    events.append({"during_kill": future.result()})
                except (OSError, ValueError) as error:
                    events.append({"during_kill_transport_error": type(error).__name__})
            second = wait_for(leader, "majority elects replacement")
            result = propose(second, b"after-leader-kill")
            assert result["status"] == 200, result
            events.append({"replacement_leader": second, "majority_write": result})
            start(first)
            recovered = wait_for(converged, "restarted peer catches up")
            events.append({"restarted_peer": first, "converged_commit": recovered[first]["commit_index"]})
            active = wait_for(leader, "leader before minority test")
            others = [node for node in ids if node != active]
            for node in others:
                stop(node)
            result = propose(active, b"minority-outcome-unknown")
            assert result["status"] == 504, f"expected quorum wait to expire: {result}"
            events.append({"minority_write": result})
            for node in others:
                start(node)
            wait_for(leader, "majority returns")
            before_restart = wait_for(converged, "majority recovery")
            # Commit knowledge is volatile; a new leader's current-term no-op
            # re-establishes the committed prefix after all processes restart.
            for node in ids:
                stop(node)
            for node in ids:
                start(node)
            wait_for(leader, "full restart election")
            final = wait_for(converged, "full restart preserves acknowledged entries")
            report.update(passed=True, acknowledged=acknowledged, before_full_restart=before_restart, final=final)
        finally:
            for node in list(processes):
                stop(node)
            for log in logs:
                log.close()
            report["process_logs"] = {node: (root / f"{node}.log").read_text(encoding="utf-8") for node in ids}
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8")
    print(f"PASS: leader failover, catch-up, minority rejection, full restart; {len(acknowledged)} acknowledged commands; {args.output}")


if __name__ == "__main__":
    main()
