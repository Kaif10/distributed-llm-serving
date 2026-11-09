"""Exercise gateway controller failover, drain, quorum loss, and recovery with real CPU inference.

Uses already installed pinned Windows llama.cpp/model assets; performs no downloads.
"""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from check_real_health import first_content
from check_real_inference import ROOT, get_json, measure, open_stream
from check_real_pool import service


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/controller-failover/results.json")
    parser.add_argument("--checkpoints", action="store_true", help="capture the peer that is restarted after quorum loss")
    parser.add_argument("--retention", action="store_true", help="exercise retained-image transfer and checkpoint-backed quorum recovery")
    parser.add_argument("--live", action="store_true", help="print live actions, state, and generated tokens")
    args = parser.parse_args()
    if args.checkpoints and args.retention:
        parser.error("choose checkpoints or retention")
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    assets = json.loads((ROOT / "config/llama-cpu.json").read_text(encoding="utf-8"))
    engine = ROOT / ".tools" / ("llama-" + assets["release"]) / "llama-server.exe"
    model = ROOT / ".tools/models" / assets["model_file"]
    assert hashlib.sha256(model.read_bytes()).hexdigest() == assets["model_sha256"], "model checksum mismatch"
    reservations = [socket.socket() for _ in range(9)]
    for sock in reservations:
        sock.bind(("127.0.0.1", 0))
    ports = [sock.getsockname()[1] for sock in reservations]
    for sock in reservations:
        sock.close()
    ids = ["a", "b", "c"]
    clients = {node: f"http://127.0.0.1:{ports[i+3]}" for i, node in enumerate(ids)}
    backend, gateway, admin = [f"http://127.0.0.1:{port}" for port in ports[6:]]
    members = ",".join(f"{node}=http://127.0.0.1:{ports[i]}" for i, node in enumerate(ids))
    payload = json.loads((ROOT / "examples/real-stream.json").read_text(encoding="utf-8"))
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
              "assets": assets, "platform": platform.platform(), "python": platform.python_version(),
              "processor": os.environ.get("PROCESSOR_IDENTIFIER", platform.processor()),
              "logical_cpus": os.cpu_count(), "commands": [], "observations": [],
              "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in
                                  {"gateway": ROOT / "bin/gateway.exe", "raftpeer": ROOT / "bin/raftpeer.exe", "engine": engine}.items()}}

    def show(*items):
        if args.live:
            print(*items, flush=True)

    def generation(label):
        show("\n"+label+": actual generated tokens ->")
        sample = measure(gateway, dict(payload, max_tokens=16), on_content=(lambda text: print(text, end="", flush=True)) if args.live else None)
        show("\nCompleted SSE stream:", sample["done"], "finish:", sample["finish_reason"])
        return sample

    def wait_for(check, label):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                value = check()
                if value:
                    return value
            except (OSError, ValueError):
                pass
            time.sleep(.03)
        raise RuntimeError("Timed out: " + label)

    def rejected(path, body=None):
        req = urllib.request.Request(gateway + path, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=3):
                raise AssertionError("expected HTTP 503")
        except urllib.error.HTTPError as error:
            code = error.code
            error.close()
            assert code == 503, code
            return code

    processes, commands = {}, {}

    def leader():
        found = [node for node, proc in processes.items() if proc.poll() is None and
                 get_json(clients[node], "/status")["node"]["Role"] == "leader"]
        return found[0] if len(found) == 1 else None

    def config_when(predicate):
        state = get_json(admin, "/config")
        return state if predicate(state) else None

    def put(node, key, revision, draining):
        update = {"request_id": key, "expected_revision": revision,
                  "workers": [{"id": "worker-a", "origin": backend, "draining": draining}]}
        req = urllib.request.Request(clients[node] + "/config", data=json.dumps(update).encode(),
                                     headers={"Content-Type": "application/json"}, method="PUT")
        with urllib.request.urlopen(req, timeout=5) as response:
            return json.load(response)

    try:
        with tempfile.TemporaryDirectory(prefix="controller-fleet-") as temporary, contextlib.ExitStack() as stack:
            engine_command = [str(engine), "-m", str(model), "--host", "127.0.0.1", "--port", str(ports[6]),
                              "--alias", "smollm2-135m", "-c", "2048", "-np", "1", "-t", "2", "-ngl", "0", "--no-ui", "--slots"]
            result["commands"].append(engine_command)
            engine_process = stack.enter_context(service(engine_command, backend + "/health", output.parent / "engine.log"))
            show("READY real llama.cpp engine PID", engine_process.pid, backend)
            for i, node in enumerate(ids):
                command = [str(ROOT / "bin/raftpeer.exe"), "-configuration", "-id", node, "-peers", members,
                           "-client-listen", f"127.0.0.1:{ports[i+3]}"]
                if args.retention:
                    command += ["-retained-dir", str(Path(temporary) / node), "-retained-group", "real-failover-check"]
                else:
                    command += ["-journal", str(Path(temporary) / f"{node}.wal")]
                if args.checkpoints:
                    command += ["-checkpoint-journal", str(Path(temporary) / f"{node}.checkpoint.wal"), "-checkpoint-group", "real-failover-check"]
                commands[node] = command
                result["commands"].append(command)
                processes[node] = stack.enter_context(service(command, clients[node] + "/status", output.parent / f"peer-{node}.log"))
                show("READY controller", node, "PID", processes[node].pid, clients[node])
            initial = wait_for(leader, "initial controller leader")
            assert put(initial, "initial", 0, False)["revision"] == 1
            show("ELECTED controller leader:", initial, "| committed configuration revision 1")
            order = [initial] + [node for node in ids if node != initial]
            gateway_command = [str(ROOT / "bin/gateway.exe"), "-listen", f"127.0.0.1:{ports[7]}",
                               "-admin-listen", f"127.0.0.1:{ports[8]}", "-backend", backend,
                               "-controllers", ",".join(clients[node] for node in order), "-config-poll", "100ms",
                               "-config-timeout", "900ms", "-config-max-age", "3s", "-request-timeout", "60s"]
            result["commands"].append(gateway_command)
            gateway_process = stack.enter_context(service(gateway_command, gateway + "/readyz", output.parent / "gateway.log"))
            show("READY gateway PID", gateway_process.pid, gateway)
            result["initial_applied"] = wait_for(lambda: config_when(lambda s: s["fresh"] and s["applied_revision"] == 1
                                                    and s["controller_origin"] == clients[initial]), "initial applied source")
            result["initial_generation"] = generation("Before failure")
            started = time.monotonic()
            show("\nKILL controller leader", initial, "PID", processes[initial].pid)
            processes[initial].kill()
            processes[initial].wait(timeout=5)
            replacement = wait_for(leader, "surviving majority elects leader")
            result["after_failover"] = wait_for(lambda: config_when(lambda s: s["fresh"] and s["applied_revision"] == 1
                                                   and s["controller_origin"] == clients[replacement]), "gateway switches controller")
            result["observations"].append({"killed_leader": initial, "replacement": replacement,
                                            "observed_failover_ms": round((time.monotonic()-started)*1000, 3)})
            show("ELECTED replacement:", replacement, "| gateway confirmed source:", result["after_failover"]["controller_origin"])
            assert put(replacement, "drain", 1, True)["revision"] == 2
            result["drain_applied"] = wait_for(lambda: config_when(lambda s: s["fresh"] and s["applied_revision"] == 2), "drain applied")
            assert get_json(admin, "/workers")[0]["draining"]
            result["drain_rejection"] = rejected("/v1/chat/completions", payload)
            show("Drain committed/applied revision 2. New generation HTTP:", result["drain_rejection"])
            assert put(replacement, "resume", 2, False)["revision"] == 3
            wait_for(lambda: config_when(lambda s: s["fresh"] and s["applied_revision"] == 3), "resume applied")
            show("Resume committed/applied revision 3")
            result["generation_after_failover"] = generation("After controller failover")
            if args.retention:
                req = urllib.request.Request(clients[replacement] + "/retain", data=b"", method="POST")
                with urllib.request.urlopen(req, timeout=5) as captured:
                    result["retained_capture"] = json.load(captured)
                processes[initial] = stack.enter_context(service(commands[initial], clients[initial] + "/status", output.parent / f"peer-{initial}-catchup.log"))
                def snapshot_caught_up():
                    state = get_json(clients[initial], "/status")
                    return state if state["retention"]["installed_index"] >= result["retained_capture"]["base_index"] and state["configuration"]["revision"] == 3 else None
                result["snapshot_caught_up_peer"] = wait_for(snapshot_caught_up, "old leader installs snapshot and catches up")
                show("Old leader restarted; snapshot installed:", result["snapshot_caught_up_peer"]["retention"]["installed_index"], "| current revision:", result["snapshot_caught_up_peer"]["configuration"]["revision"])
                processes[initial].kill()
                processes[initial].wait(timeout=5)
                show("Stopped old leader again; remaining two controllers still form quorum")
            other = next(node for node in ids if node not in (initial, replacement))
            if args.checkpoints:
                wait_for(lambda: get_json(clients[other], "/status")["configuration"]["revision"] == 3, "checkpoint peer applies revision 3")
                req = urllib.request.Request(clients[other] + "/checkpoint", data=b"", method="POST")
                with urllib.request.urlopen(req, timeout=5) as captured:
                    result["captured_checkpoint"] = json.load(captured)
            connection, response = open_stream(gateway, dict(payload, max_tokens=1536, ignore_eos=True))
            try:
                stream = first_content(response)
                assert get_json(admin, "/workers")[0]["inflight"] == 1
                processes[other].kill()
                processes[other].wait(timeout=5)
                show("\nKILL controller", other, "| majority lost while a real generation is active")
                result["expired"] = wait_for(lambda: config_when(lambda s: not s["fresh"]), "view expires without majority")
                result["worker_at_expiry"] = get_json(admin, "/workers")
                assert result["worker_at_expiry"][0]["inflight"] == 1, "long generation finished before expiry observation"
                result["readiness_when_expired"] = rejected("/readyz")
                show("Gateway fresh:", result["expired"]["fresh"], "| readiness HTTP:", result["readiness_when_expired"], "| active worker requests:", result["worker_at_expiry"][0]["inflight"])
                done, finish, chunks = False, None, 0
                for event in stream:
                    if event is None:
                        done = True
                        break
                    assert not event.get("error"), event
                    chunks += 1
                    if chunks % 256 == 0:
                        show("Existing real stream still arriving:", chunks, "chunks after first content")
                    for choice in event.get("choices", []):
                        finish = choice.get("finish_reason") or finish
                assert done and finish is not None, "active real stream did not complete normally"
                result["long_stream"] = {"done": done, "finish_reason": finish, "chunks_after_first_content": chunks}
                result["active_stream_survived_expiry"] = True
                show("Existing real stream completed:", done, "| finish:", finish, "| chunks:", chunks)
            finally:
                response.close()
                connection.close()
            wait_for(lambda: not get_json(admin, "/workers")[0]["inflight"], "active stream released")
            result["idle_generation_when_expired"] = rejected("/v1/chat/completions", payload)
            show("Worker idle, quorum still absent: new generation HTTP", result["idle_generation_when_expired"])
            recovering = initial if args.retention else other
            processes[recovering] = stack.enter_context(service(commands[recovering], clients[recovering] + "/status", output.parent / f"peer-{recovering}-restarted.log"))
            if args.retention:
                expected = result["retained_capture"]["base_index"]
                result["retained_restarted_peer"] = wait_for(lambda: (state if (state := get_json(clients[recovering], "/status"))["retention"]["recovered_index"] == expected else None), "durable installed checkpoint restored")
                show("RESTART controller", recovering, "PID", processes[recovering].pid, "| restored durable boundary", expected)
            if args.checkpoints:
                expected = result["captured_checkpoint"]["published"]["position"]["index"]
                def restored_checkpoint():
                    state = get_json(clients[other], "/status")
                    return state if state["checkpoint"]["recovery_index"] == expected and not state["checkpoint"]["recovery_pending"] else None
                result["restored_checkpoint_peer"] = wait_for(restored_checkpoint, "checkpoint prefix installed after commitment")
            result["after_quorum_recovery"] = wait_for(lambda: config_when(lambda s: s["fresh"] and s["applied_revision"] == 3), "same-revision reconfirmation")
            show("Quorum recovered; gateway fresh:", result["after_quorum_recovery"]["fresh"], "| applied revision:", result["after_quorum_recovery"]["applied_revision"])
            result["generation_after_recovery"] = generation("After checkpoint-backed quorum recovery")
            if args.retention:
                result["historical_retry_after_recovery"] = put(replacement, "initial", 0, False)
                assert result["historical_retry_after_recovery"]["revision"] == 1
                show("Historical request retry returns original revision:", result["historical_retry_after_recovery"]["revision"], "| current configuration remains revision 3")
            result["controller_statuses"] = {node: get_json(clients[node], "/status") for node, proc in processes.items() if proc.poll() is None}
            result["passed"] = True
            print("PASS: real CPU generation, controller leader failover, drain, expiry with stream completion, and quorum recovery", flush=True)
    except Exception as error:
        result.update(passed=False, error=str(error))
        raise
    finally:
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
