"""Real peer processes, partitioning HTTP relays, configuration commit/read/replay checks.

Usage: python scripts/replicated_config_check.py bin/raftpeer.exe --output artifacts/replicated-config/run.json
"""
import argparse
import concurrent.futures
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request


def request(port, path, method="GET", value=None, timeout=7):
    data = None if value is None else json.dumps(value).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--checkpoints", action="store_true", help="capture/recover one peer; compare with full-replay peers")
    args = parser.parse_args()
    binary = args.binary.resolve()
    ids = ["a", "b", "c"]
    reservations = [socket.socket() for _ in range(9)]
    for sock in reservations:
        sock.bind(("127.0.0.1", 0))
    ports = [sock.getsockname()[1] for sock in reservations]
    for sock in reservations:
        sock.close()
    backend = dict(zip(ids, ports[:3]))
    clients = dict(zip(ids, ports[3:6]))
    advertised = dict(zip(ids, ports[6:]))
    isolated = set()
    partition_lock = threading.Lock()
    processes, files, relays, relay_threads, events = {}, [], [], [], []
    report = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "working_tree": subprocess.check_output(["git", "status", "--porcelain"], text=True),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "command": [sys.executable, *sys.argv], "platform": platform.platform(),
              "python": platform.python_version(), "events": events,
              "scope": "three real peer processes, same-host HTTP, controlled relay partitions; no LLM traffic"}

    def relay_handler(destination):
        class Relay(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                source = json.loads(data)["From"]
                with partition_lock:
                    blocked = source in isolated or destination in isolated
                code = 503
                if not blocked:
                    req = urllib.request.Request(f"http://127.0.0.1:{backend[destination]}/raft/message", data=data)
                    try:
                        with urllib.request.urlopen(req, timeout=.5) as response:
                            code = response.status
                    except urllib.error.HTTPError as error:
                        code = error.code
                    except OSError:
                        pass
                self.send_response(code)
                self.send_header("Content-Length", "0")
                self.end_headers()
        return Relay

    with tempfile.TemporaryDirectory(prefix="fleet-config-") as directory:
        root = Path(directory)
        peers = ",".join(f"{node}=http://127.0.0.1:{advertised[node]}" for node in ids)

        def start(node):
            log = open(root / f"{node}.log", "ab")
            files.append(log)
            command = [str(binary), "-configuration", "-id", node, "-peers", peers,
                "-peer-listen", f"127.0.0.1:{backend[node]}", "-client-listen", f"127.0.0.1:{clients[node]}",
                "-journal", str(root / f"{node}.wal")]
            if args.checkpoints:
                command += ["-checkpoint-journal", str(root / f"{node}.checkpoint.wal"), "-checkpoint-group", "replicated-config-check"]
            processes[node] = subprocess.Popen(command, stdout=log, stderr=log)

        def stop(node):
            process = processes.pop(node)
            process.kill()
            process.wait(timeout=5)

        def status(node):
            code, state = request(clients[node], "/status")
            assert code == 200, (code, state)
            return state

        def wait_for(check, label):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                for node, process in processes.items():
                    assert process.poll() is None, (node, (root / f"{node}.log").read_text(encoding="utf-8"))
                try:
                    value = check()
                    if value:
                        return value
                except (OSError, ValueError):
                    pass
                time.sleep(.05)
            raise AssertionError(f"timeout: {label}")

        def leader(eligible=None):
            found = [node for node in (eligible or list(processes)) if status(node)["node"]["Role"] == "leader"]
            return found[0] if len(found) == 1 else None

        workers = [{"id": "worker", "origin": "http://127.0.0.1:8081", "draining": True}]
        initial = {"request_id": "initial-drain", "expected_revision": 0, "workers": workers}

        def converged(revision):
            states = {node: status(node) for node in processes}
            first = next(iter(states.values()))
            if any(s["configuration"]["revision"] != revision or s["applied_index"] != s["commit_index"] or
                   s["log"] != first["log"] or s["commit_index"] != len(s["log"]) for s in states.values()):
                return None
            assert all(s["configuration"] == first["configuration"] for s in states.values()), "application diverged"
            return states

        try:
            for node in ids:
                server = ThreadingHTTPServer(("127.0.0.1", advertised[node]), relay_handler(node))
                relays.append(server)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                relay_threads.append(thread)
                thread.start()
            for node in ids:
                start(node)
            first = wait_for(leader, "initial leader")
            code, original = request(clients[first], "/config", "PUT", initial)
            assert code == 200 and original == {"revision": 1, "workers": workers}, (code, original)
            before_reads = wait_for(lambda: converged(1), "initial application")
            sizes_before = {node: (root / f"{node}.wal").stat().st_size for node in ids}
            read_count = 128
            for _ in range(read_count):
                code, snapshot = request(clients[first], "/config")
                assert (code, snapshot) == (200, original), (code, snapshot)
            after_reads = wait_for(lambda: converged(1), "reads leave application converged")
            sizes_after = {node: (root / f"{node}.wal").stat().st_size for node in ids}
            assert sizes_before == sizes_after, (sizes_before, sizes_after)
            assert all(after_reads[node]["log"] == before_reads[node]["log"] and
                       after_reads[node]["node"]["Term"] == before_reads[node]["node"]["Term"] and
                       after_reads[node]["pending_reads"] == 0 for node in ids), "reads grew log, changed term, or leaked waiters"
            events.append({"repeated_reads": read_count, "journal_bytes_before": sizes_before,
                           "journal_bytes_after": sizes_after,
                           "log_entries_before": {node: len(before_reads[node]["log"]) for node in ids},
                           "log_entries_after": {node: len(after_reads[node]["log"]) for node in ids}})
            events.append({"initial_leader": first, "initial_result": original})
            with partition_lock:
                isolated.add(first)
            others = [node for node in ids if node != first]
            second = wait_for(lambda: leader(others), "majority replacement")
            resume = {"request_id": "resume", "expected_revision": 1, "workers": [{**workers[0], "draining": False}]}
            code, resumed = request(clients[second], "/config", "PUT", resume)
            assert code == 200 and resumed["revision"] == 2, (code, resumed)
            # Invoke reads only AFTER the newer majority update has returned.
            # The old process is alive and still calls itself leader in its old term.
            assert status(first)["node"]["Role"] == "leader"
            minority_update = {"request_id": "isolated-update", "expected_revision": 1, "workers": []}
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                old_read = pool.submit(request, clients[first], "/config")
                old_write = pool.submit(request, clients[first], "/config", "PUT", minority_update)
                good_read = pool.submit(request, clients[second], "/config")
                good, stale, minority = good_read.result(), old_read.result(), old_write.result()
            assert good == (200, resumed), good
            assert stale[0] == 504, f"isolated former leader served stale configuration: {stale}"
            assert minority[0] == 504, f"minority acknowledged update: {minority}"
            assert status(first)["configuration"] == original, "applied uncommitted minority update"
            events.append({"replacement_leader": second, "majority_read": good, "isolated_former_leader_read": stale,
                           "isolated_update": minority,
                           "old_diagnostic_configuration": status(first)["configuration"]})
            with partition_lock:
                isolated.clear()
            wait_for(lambda: converged(2), "heal and apply")
            current = wait_for(leader, "one leader after healing")
            code, body = request(clients[current], "/config", "PUT", minority_update)
            assert code == 409, (code, body)
            code, retried = request(clients[current], "/config", "PUT", initial)
            assert (code, retried) == (200, original), (code, retried)
            # Two valid commands race with the same expected revision. CAS is
            # evaluated in committed order, so exactly one may advance revision.
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(request, clients[current], "/config", "PUT",
                    {"request_id": f"race-{i}", "expected_revision": 2, "workers": workers if i else []}) for i in range(2)]
                outcomes = [future.result() for future in futures]
            assert sorted(code for code, _ in outcomes) == [200, 409], outcomes
            expected = next(state for code, state in outcomes if code == 200)
            events.append({"retry_original_result": retried, "concurrent_CAS": outcomes})
            wait_for(lambda: converged(3), "CAS application")
            if args.checkpoints:
                capture_node = ids[1]
                code, capture = request(clients[capture_node], "/checkpoint", "POST")
                assert code == 200 and capture["published"]["generation"] == 1, (code, capture)
                selected_index = capture["published"]["position"]["index"]
                events.append({"captured_peer": capture_node, "checkpoint": capture})
            # Force recovery of both application and successful request-ID results.
            for node in ids:
                stop(node)
            for node in ids:
                start(node)
            current = wait_for(leader, "full restart leader")
            final = wait_for(lambda: converged(3), "full replay")
            if args.checkpoints:
                recovered = final[capture_node]["checkpoint"]
                assert recovered["recovery_index"] == selected_index and recovered["recovery_verified"] and not recovered["recovery_pending"], recovered
                baseline = ids[0]
                assert final[baseline]["checkpoint"]["recovery_index"] == 0, "baseline did not use full replay"
                code, full_image = request(clients[baseline], "/checkpoint", "POST")
                assert code == 200, (code, full_image)
                code, restored_image = request(clients[capture_node], "/checkpoint", "POST")
                assert code == 200, (code, restored_image)
                assert full_image["published"]["position"] == restored_image["published"]["position"], (full_image, restored_image)
                assert full_image["published"]["sha256"] == restored_image["published"]["sha256"], "checkpoint/suffix and full replay retry tables diverged"
                events.append({"restored_checkpoint": recovered, "full_replay_image": full_image, "restored_image": restored_image})
            code, state = request(clients[current], "/config")
            assert (code, state) == (200, expected), (code, state)
            code, state = request(clients[current], "/config", "PUT", initial)
            assert (code, state) == (200, original), (code, state)
            changed = {**initial, "expected_revision": 3}
            code, _ = request(clients[current], "/config", "PUT", changed)
            assert code == 409, "reused ID with altered payload accepted"
            final = wait_for(lambda: converged(3), "post-restart retries applied")
            follower = next(node for node in ids if node != current)
            code, body = request(clients[follower], "/config")
            assert code == 503, "follower served a local configuration read"
            report.update(passed=True, final=final, expected_configuration=expected,
                          recovered_retry=state, follower_read={"status": code, "body": body})
        finally:
            for node in list(processes):
                stop(node)
            for server in relays:
                server.shutdown()
                server.server_close()
            for thread in relay_threads:
                thread.join(timeout=2)
            for file in files:
                file.close()
            report["process_logs"] = {node: (root / f"{node}.log").read_text(encoding="utf-8") if (root / f"{node}.log").exists() else "" for node in ids}
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8")
    print(f"PASS: reads leave journals unchanged, commit/apply, stale-leader read blocked, CAS, retry replay, full restart; {args.output}")


if __name__ == "__main__":
    main()
