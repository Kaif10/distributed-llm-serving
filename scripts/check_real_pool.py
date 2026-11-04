"""Start two real CPU model servers and validate pool admission for both routing policies."""

import contextlib
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import time

from check_real_inference import ROOT, events, get_json, measure, open_stream


@contextlib.contextmanager
def service(command, health, log_path):
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, stdout=log, stderr=log,
                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        try:
            for _ in range(300):
                if process.poll() is not None:
                    raise RuntimeError("Service exited; inspect " + str(log_path))
                try:
                    get_json(health, "")
                    break
                except (OSError, ValueError):
                    time.sleep(.1)
            else:
                raise RuntimeError("Service startup timed out")
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def slot_states():
    return [get_json("http://127.0.0.1:" + str(port), "/slots")[0] for port in (18081, 18082)]


def wait_idle():
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        states = slot_states()
        if all(not s["is_processing"] for s in states):
            return states
        time.sleep(.02)
    raise RuntimeError("Real engines did not release slots")


def exercise(policy, payload):
    gateway = "http://127.0.0.1:18080"
    held = []
    result = {"policy": policy}
    try:
        wait_idle()
        for _ in range(2):
            connection, response = open_stream(gateway, dict(payload, max_tokens=1024, ignore_eos=True))
            held.append((connection, response))
            for event in events(response):
                if event is None:
                    raise RuntimeError("Generation ended before saturation")
                if any(c.get("delta", {}).get("content") for c in event.get("choices", [])):
                    break
            else:
                raise RuntimeError("No generated text")
        result["busy_slots"] = slot_states()
        if not all(s["is_processing"] for s in result["busy_slots"]):
            raise RuntimeError("Both real engines must be actively generating")
        connection = http.client.HTTPConnection("127.0.0.1", 18080, timeout=3)
        try:
            start = time.perf_counter()
            connection.request("POST", "/v1/chat/completions", json.dumps(payload), {"Content-Type": "application/json"})
            response = connection.getresponse()
            result["overload"] = {"status": response.status, "retry_after": response.getheader("Retry-After"),
                                  "body": response.read().decode(), "elapsed_ms": round((time.perf_counter()-start)*1000, 3)}
            response.close()
            if result["overload"]["status"] != 503 or result["overload"]["retry_after"] != "1":
                raise RuntimeError("Expected admission rejection while both engines busy")
        finally:
            connection.close()
    finally:
        for connection, response in held:
            response.close()
            connection.close()
    result["idle_after_disconnect"] = wait_idle()
    # Engine capacity can return just before the gateway handler releases its reservation.
    time.sleep(.1)
    result["after_release"] = measure(gateway, dict(payload, max_tokens=16))
    result["passed"] = True
    print("PASS", policy, ": two busy real engines, third request 503, cancellation releases slots, new generation completes", flush=True)
    return result


def main():
    output = ROOT / "artifacts/real-pool"
    output.mkdir(parents=True, exist_ok=True)
    # Refuse to share ports with existing services. Every child below is owned by this run.
    for port in (18080, 18081, 18082):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    config = json.loads((ROOT / "config/llama-cpu.json").read_text())
    engine = ROOT / ".tools" / ("llama-" + config["release"]) / "llama-server.exe"
    model = ROOT / ".tools/models" / config["model_file"]
    payload = json.loads((ROOT / "examples/real-stream.json").read_text())
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
              "assets": config, "request": payload, "runs": [], "commands": []}
    try:
        with contextlib.ExitStack() as stack:
            for port in (18081, 18082):
                command = [str(engine), "-m", str(model), "--host", "127.0.0.1", "--port", str(port),
                           "--alias", "smollm2-135m", "-c", "2048", "-np", "1", "-t", "2", "-ngl", "0",
                           "--slots", "--no-ui", "--no-cache-prompt"]
                result["commands"].append(command)
                stack.enter_context(service(command, f"http://127.0.0.1:{port}/health", output / f"engine-{port}.log"))
            for policy in ("round-robin", "least-inflight"):
                command = [str(ROOT / "bin/gateway.exe"), "-listen", "127.0.0.1:18080", "-backends",
                           "http://127.0.0.1:18081,http://127.0.0.1:18082", "-policy", policy,
                           "-max-inflight", "1", "-request-timeout", "60s"]
                result["commands"].append(command)
                with service(command, "http://127.0.0.1:18080/readyz", output / f"gateway-{policy}.log"):
                    result["runs"].append(exercise(policy, payload))
        result["passed"] = True
    except Exception as err:
        result["passed"] = False
        result["error"] = str(err)
        raise
    finally:
        (output / "results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
