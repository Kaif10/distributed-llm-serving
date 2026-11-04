"""Exercise draining, process loss, and recovery against two real CPU model servers."""

import contextlib
import http.client
import json
import socket
import subprocess
import time
import urllib.error
import urllib.request

from check_real_inference import ROOT, events, get_json, measure, open_stream
from check_real_pool import service

ADMIN = "http://127.0.0.1:18090"
GATEWAY = "http://127.0.0.1:18080"


def wait_states(predicate):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        states = get_json(ADMIN, "/workers")
        if predicate(states):
            return states
        time.sleep(.02)
    raise RuntimeError("Gateway state did not converge: " + repr(states))


def admin_action(worker, action):
    request = urllib.request.Request(f"{ADMIN}/workers/{worker}/{action}", method="POST", data=b"")
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)


def first_content(response):
    stream = events(response)
    for event in stream:
        if event is None:
            break
        if any(c.get("delta", {}).get("content") for c in event.get("choices", [])):
            return stream
    raise RuntimeError("No content before completion")


def main():
    output = ROOT / "artifacts/real-health"
    output.mkdir(parents=True, exist_ok=True)
    for port in (18080, 18081, 18082, 18090):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    config = json.loads((ROOT / "config/llama-cpu.json").read_text())
    engine = ROOT / ".tools" / ("llama-" + config["release"]) / "llama-server.exe"
    model = ROOT / ".tools/models" / config["model_file"]
    payload = json.loads((ROOT / "examples/real-stream.json").read_text())
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
              "assets": config, "commands": [], "request": payload}
    try:
        with contextlib.ExitStack() as stack:
            engines = []
            for port in (18081, 18082):
                command = [str(engine), "-m", str(model), "--host", "127.0.0.1", "--port", str(port),
                           "--alias", "smollm2-135m", "-c", "2048", "-np", "1", "-t", "2", "-ngl", "0",
                           "--slots", "--no-ui", "--no-cache-prompt"]
                result["commands"].append(command)
                engines.append(stack.enter_context(service(command, f"http://127.0.0.1:{port}/health", output / f"engine-{port}.log")))
            command = [str(ROOT / "bin/gateway.exe"), "-listen", "127.0.0.1:18080", "-admin-listen", "127.0.0.1:18090",
                       "-backends", "http://127.0.0.1:18081,http://127.0.0.1:18082", "-policy", "round-robin",
                       "-max-inflight", "1", "-request-timeout", "60s", "-health-interval", "100ms", "-health-timeout", "100ms"]
            result["commands"].append(command)
            stack.enter_context(service(command, GATEWAY + "/healthz", output / "gateway.log"))
            result["initial"] = wait_states(lambda s: all(w["healthy"] for w in s))
            conn, response = open_stream(GATEWAY, dict(payload, max_tokens=256, ignore_eos=True))
            try:
                stream = first_content(response)
                result["drain_ack"] = admin_action(0, "drain")
                if result["drain_ack"]["inflight"] != 1 or result["drain_ack"]["drained"]:
                    raise RuntimeError("Expected a drain acknowledgement while real stream is active")
                result["other_worker_generation"] = measure(GATEWAY, dict(payload, max_tokens=16))
                if not any(event is None for event in stream):
                    raise RuntimeError("Drain truncated the original real generation")
                result["original_stream_completed"] = True
            finally:
                response.close(); conn.close()
            result["drained"] = wait_states(lambda s: s[0]["drained"])
            admin_action(0, "resume")
            conn, response = open_stream(GATEWAY, dict(payload, max_tokens=1024, ignore_eos=True))
            try:
                stream = first_content(response)
                states = get_json(ADMIN, "/workers")
                if states[0]["inflight"] != 1:
                    raise RuntimeError("Failure experiment must target worker 0")
                engines[0].terminate(); engines[0].wait(timeout=5)
                try:
                    completed = any(event is None for event in stream)
                    result["failed_stream"] = "EOF without DONE" if not completed else "unexpected completion"
                except (http.client.HTTPException, OSError) as err:
                    completed = False
                    result["failed_stream"] = type(err).__name__
                if completed:
                    raise RuntimeError("Killed worker's stream appeared successful")
            finally:
                response.close(); conn.close()
            result["after_worker_loss"] = wait_states(lambda s: not s[0]["healthy"] and s[1]["healthy"])
            result["survivor_generations"] = [measure(GATEWAY, dict(payload, max_tokens=16)) for _ in range(2)]
            engines[1].terminate(); engines[1].wait(timeout=5)
            result["all_down"] = wait_states(lambda s: all(not w["healthy"] for w in s))
            for path, body in [("/readyz", None), ("/v1/chat/completions", json.dumps(payload).encode())]:
                try:
                    with urllib.request.urlopen(urllib.request.Request(GATEWAY+path, data=body), timeout=3):
                        raise RuntimeError("Expected 503 when all workers are down")
                except urllib.error.HTTPError as err:
                    if err.code != 503:
                        raise
                    err.close()
            result["all_down_rejected"] = True
            stack.enter_context(service(result["commands"][0], "http://127.0.0.1:18081/health", output / "engine-restarted.log"))
            result["recovered"] = wait_states(lambda s: s[0]["healthy"] and not s[1]["healthy"])
            result["recovery_generation"] = measure(GATEWAY, dict(payload, max_tokens=16))
            result["passed"] = True
            print("PASS: real stream survives drain; worker loss truncates its stream; survivor serves; all-down rejects; restart recovers", flush=True)
    except Exception as err:
        result["passed"] = False
        result["error"] = str(err)
        raise
    finally:
        (output / "results.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
