"""Check a running llama.cpp backend and gateway; record a small sequential CPU baseline.

No services are started and no downloads are performed by this script.
"""

import argparse
import http.client
import json
from pathlib import Path
import platform
import random
import subprocess
import time
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parents[1]


def get_json(origin, path):
    with urllib.request.urlopen(origin.rstrip("/") + path, timeout=5) as response:
        return json.load(response)


def open_stream(origin, payload, headers=None):
    url = urllib.parse.urlsplit(origin)
    if url.scheme != "http" or not url.hostname or url.path not in ("", "/"):
        raise ValueError("Experiment expects an HTTP origin without a path")
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=60)
    try:
        connection.request("POST", "/v1/chat/completions", json.dumps(payload), {"Content-Type": "application/json", **(headers or {})})
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {response.read()[:500]!r}")
        if "text/event-stream" not in response.getheader("Content-Type", ""):
            raise RuntimeError("Expected SSE response")
        return connection, response
    except BaseException:
        connection.close()
        raise


def events(response):
    while line := response.readline():
        if not line.startswith(b"data: "):
            continue
        data = line[6:].strip()
        if data == b"[DONE]":
            yield None
            return
        yield json.loads(data)


def measure(origin, payload, headers=None):
    started = time.perf_counter()
    connection, response = open_stream(origin, payload, headers)
    first = None
    parts = []
    usage = None
    finish = None
    done = False
    try:
        for event in events(response):
            if event is None:
                done = True
                break
            if event.get("error"):
                raise RuntimeError(str(event["error"]))
            usage = event.get("usage") or usage
            for choice in event.get("choices", []):
                content = choice.get("delta", {}).get("content") or ""
                if content:
                    if first is None:
                        first = time.perf_counter()
                    parts.append(content)
                finish = choice.get("finish_reason") or finish
        elapsed = time.perf_counter() - started
        if first is None or not done or finish is None:
            raise RuntimeError("Incomplete stream or no generated content")
        return {"ttft_ms": round((first-started)*1000, 3), "completion_ms": round(elapsed*1000, 3),
                "text": "".join(parts), "usage": usage, "finish_reason": finish, "done": done}
    finally:
        response.close()
        connection.close()


def cancellation_check(backend, gateway, payload):
    if any(slot["is_processing"] for slot in get_json(backend, "/slots")):
        raise RuntimeError("Cancellation check requires an otherwise idle engine")
    request = dict(payload, max_tokens=1024, ignore_eos=True)
    connection, response = open_stream(gateway, request)
    try:
        for event in events(response):
            if event is None:
                raise RuntimeError("Long generation finished before cancellation check")
            if any(c.get("delta", {}).get("content") for c in event.get("choices", [])):
                break
        else:
            raise RuntimeError("No generated content before disconnect")
        before = get_json(backend, "/slots")
        if not any(s["is_processing"] for s in before):
            raise RuntimeError("Engine must still be generating before disconnect")
    finally:
        response.close()
        connection.close()
    closed = time.perf_counter()
    while time.perf_counter() - closed < 5:
        after = get_json(backend, "/slots")
        if not any(s["is_processing"] for s in after):
            return {"passed": True, "release_observed_ms": round((time.perf_counter()-closed)*1000, 3),
                    "requested_tokens": 1024, "slots_before_disconnect": before, "slots_after_disconnect": after}
        time.sleep(.02)
    raise RuntimeError("Engine slot still busy five seconds after gateway client disconnect")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", default="http://127.0.0.1:18081")
    parser.add_argument("--gateway", default="http://127.0.0.1:18080")
    parser.add_argument("--trials", type=int, default=3, help="measured requests per route, after one warmup each")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/real-cpu/results.json")
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("trials must be positive")
    config = json.loads((ROOT / "config/llama-cpu.json").read_text())
    result = {"gateway_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
              "platform": platform.platform(), "backend": args.backend, "gateway": args.gateway,
              "pinned_assets": config, "samples": []}
    payload = json.loads((ROOT / "examples/real-stream.json").read_text())
    payload.update(max_tokens=32, ignore_eos=True, cache_prompt=False, stream_options={"include_usage": True})
    result["request"] = payload
    result["warmups_per_route"] = 1
    try:
        get_json(args.backend, "/health")
        get_json(args.gateway, "/healthz")
        for origin in [args.backend, args.gateway]:
            measure(origin, payload)
        order = ["direct", "gateway"] * args.trials
        random.Random(7).shuffle(order)
        result["policy_order_seed"] = 7
        for route in order:
            sample = measure(args.backend if route == "direct" else args.gateway, payload)
            sample["route"] = route
            result["samples"].append(sample)
            print(route, "TTFT", sample["ttft_ms"], "ms; complete", sample["completion_ms"], "ms", flush=True)
        result["cancellation"] = cancellation_check(args.backend, args.gateway, payload)
        print("PASS: engine slot released after client disconnected through gateway", flush=True)
        result["passed"] = True
    except Exception as err:
        result["passed"] = False
        result["error"] = str(err)
        raise
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("Raw results:", args.output, flush=True)


if __name__ == "__main__":
    main()
