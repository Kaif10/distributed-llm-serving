"""Fixed-arrival real-model load experiment with a changing hot document."""

import argparse
import concurrent.futures
import contextlib
import http.client
import json
import math
from pathlib import Path
import platform
import random
import socket
import statistics
import subprocess
import time

from bench_cache import request_for
from check_real_inference import ROOT, events, measure
from check_real_pool import service


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values)*fraction)-1)] if values else None


def shaped_request(workload, document, position, profile):
    payload = request_for(workload, document, position%16, True)
    kind = "uniform"
    if profile == "mixed":
        kind = "long" if position%4 == 0 else "short"
        if kind == "short":
            payload["messages"][0]["content"] = "\n".join(workload["documents"][document].splitlines()[:4])
        payload["max_tokens"] = 128 if kind == "long" else 16
    return kind, payload


def send_request(position, document, due, payload, ttft_objective, completion_objective, kind="uniform"):
    started = time.perf_counter()
    result = {"id": position, "document": document, "dispatch_lag_ms": (started-due)*1000,
              "admitted": False, "within_objective": False, "request_class": kind,
              "requested_output_tokens": payload["max_tokens"], "system_prompt_characters": len(payload["messages"][0]["content"])}
    conn = http.client.HTTPConnection("127.0.0.1", 18080, timeout=65)
    try:
        conn.request("POST", "/v1/chat/completions", json.dumps(payload),
                     {"Content-Type": "application/json", "X-Prefix-Key": "smollm2-v1-document-"+document+("-"+kind if kind != "uniform" else "")})
        response = conn.getresponse()
        try:
            result["status"] = response.status
            result["worker"] = response.getheader("X-Inference-Worker")
            result["admitted"] = result["worker"] is not None
            if response.status != 200:
                result.update(outcome="rejected" if response.status == 503 else "http_error",
                              body=response.read(4096).decode(errors="replace"))
                return result
            result["admitted"] = True  # Observed HTTP-200 admission, not hidden server accounting.
            first, done, finish, usage = None, False, None, None
            for event in events(response):
                if event is None:
                    done = True; break
                if event.get("error"):
                    raise RuntimeError(str(event["error"]))
                usage = event.get("usage") or usage
                for choice in event.get("choices", []):
                    if first is None and choice.get("delta", {}).get("content"):
                        first = time.perf_counter()
                    finish = choice.get("finish_reason") or finish
            if not done or first is None or finish is None or usage is None or usage.get("completion_tokens") != payload["max_tokens"]:
                raise RuntimeError("Incomplete stream, absent content/usage, or unexpected output tokens")
            completion = time.perf_counter()
            result.update(outcome="completed", ttft_ms=(first-due)*1000, completion_ms=(completion-due)*1000,
                          usage=usage, finish_reason=finish)
            result["within_objective"] = result["ttft_ms"] <= ttft_objective and result["completion_ms"] <= completion_objective
        finally:
            response.close()
    except Exception as err:
        result.update(outcome="failed", error=type(err).__name__+": "+str(err))
    finally:
        conn.close()
        result["elapsed_from_due_ms"] = (time.perf_counter()-due)*1000
    return result


def run(policy, rate, trial, args, config, workload, output):
    label = f"{trial}-{policy}-{rate:g}"
    result = {"policy": policy, "rate_rps": rate, "trial": trial, "commands": [], "calibration": []}
    engine = ROOT/".tools"/("llama-"+config["release"])/"llama-server.exe"
    origins = ["http://127.0.0.1:18081", "http://127.0.0.1:18082"]
    with contextlib.ExitStack() as stack:
        for i, origin in enumerate(origins):
            command = [str(engine), "-m", str(ROOT/".tools/models"/config["model_file"]), "--host", "127.0.0.1",
                       "--port", str(18081+i), "--alias", "smollm2-135m", "-c", "2048", "-np", "1", "-t", str(args.worker_threads[i]),
                       "-ngl", "0", "--no-ui", "--slots", "--cache-prompt", "--cache-ram", "0", "--no-cache-idle-slots"]
            result["commands"].append(command)
            stack.enter_context(service(command, origin+"/health", output/f"{label}-worker{i}.log"))
            if args.calibrate:
                for position in [1, 0]:
                    kind, probe = shaped_request(workload, "A", position, "mixed")
                    probe["cache_prompt"] = False
                    sample = measure(origin, probe)
                    if sample["usage"]["completion_tokens"] != probe["max_tokens"]:
                        raise RuntimeError("Calibration output length mismatch")
                    sample.update(worker=i, threads=args.worker_threads[i], request_class=kind)
                    result["calibration"].append(sample)
            measure(origin, {"model": "smollm2-135m", "messages": [{"role": "user", "content": "Say hello."}],
                             "stream": True, "max_tokens": 16, "ignore_eos": True, "cache_prompt": False})
        command = [str(ROOT/"bin/gateway.exe"), "-listen", "127.0.0.1:18080", "-backends", ",".join(origins),
                   "-policy", policy, "-max-inflight", "1", "-request-timeout", "60s"]
        result["commands"].append(command)
        stack.enter_context(service(command, "http://127.0.0.1:18080/readyz", output/f"{label}-gateway.log"))
        start = time.perf_counter()+.1
        futures = []
        # One thread per possible request avoids a hidden executor queue. This is
        # bounded by the validated request count, independent of response speed.
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.requests) as executor:
            for position, document in enumerate(args.documents):
                due = start+position/rate
                time.sleep(max(0, due-time.perf_counter()))
                kind, payload = shaped_request(workload, document, position, args.profile)
                futures.append(executor.submit(send_request, position, document, due,
                                               payload, args.ttft_ms, args.completion_ms, kind))
            samples = [future.result() for future in futures]
        window = args.requests/rate
        observed = max(window, time.perf_counter()-start)
        completed = [s for s in samples if s["outcome"] == "completed"]
        good = sum(s["within_objective"] for s in samples)
        tokens = sum(s["usage"]["prompt_tokens"] for s in completed)
        result.update(samples=samples, offered=args.requests, observed_admitted=sum(s["admitted"] for s in samples),
                      completed=len(completed), rejected=sum(s["outcome"] == "rejected" for s in samples),
                      failed=sum(s["outcome"] in ("failed", "http_error") for s in samples),
                      objective_met=good, objective_fraction=good/args.requests,
                      arrival_window_seconds=window, observed_seconds=observed,
                      throughput_rps=len(completed)/observed, goodput_rps=good/observed,
                      max_dispatch_lag_ms=max(s["dispatch_lag_ms"] for s in samples),
                      successful_ttft_p95_ms=percentile([s["ttft_ms"] for s in completed], .95),
                      successful_completion_p95_ms=percentile([s["completion_ms"] for s in completed], .95),
                      cached_token_fraction=sum(s["usage"]["prompt_tokens_details"]["cached_tokens"] for s in completed)/tokens if tokens else None)
        assert result["completed"]+result["rejected"]+result["failed"] == args.requests
        result["class_summary"] = {}
        for kind in sorted({s["request_class"] for s in samples}):
            rows = [s for s in samples if s["request_class"] == kind]
            result["class_summary"][kind] = {"offered": len(rows), "completed": sum(s["outcome"] == "completed" for s in rows),
                                           "rejected": sum(s["outcome"] == "rejected" for s in rows),
                                           "failed": sum(s["outcome"] in ("failed", "http_error") for s in rows),
                                           "objective_met": sum(s["within_objective"] for s in rows)}
        (output/f"{label}.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")
        print(label, "completed", result["completed"], "rejected", result["rejected"], "failed", result["failed"],
              "objective", good, "goodput", round(result["goodput_rps"],3), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=24)
    parser.add_argument("--rates", type=float, nargs="+", default=[1, 4])
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--profile", choices=["uniform", "mixed"], default="uniform")
    parser.add_argument("--worker-threads", type=int, nargs=2, default=[2, 2])
    parser.add_argument("--calibrate", action="store_true", help="measure each engine directly before warmup")
    parser.add_argument("--ttft-ms", type=float, default=2000)
    parser.add_argument("--completion-ms", type=float, default=3000)
    parser.add_argument("--output-dir", type=Path, default=ROOT/"artifacts/load-baseline")
    args = parser.parse_args()
    if any(n < 1 or n > 8 for n in args.worker_threads):
        parser.error("worker thread counts must be between 1 and 8")
    if not 4 <= args.requests <= 64 or args.trials < 1 or any(not math.isfinite(r) or r <= 0 for r in args.rates) or not 0 < args.ttft_ms <= args.completion_ms or not math.isfinite(args.completion_ms):
        parser.error("require 4..64 requests, positive finite rates/objectives, and positive trials")
    for port in (18080, 18081, 18082):
        with socket.socket() as probe: probe.bind(("127.0.0.1", port))
    output = args.output_dir.resolve(); output.mkdir(parents=True, exist_ok=True)
    config = json.loads((ROOT/"config/llama-cpu.json").read_text())
    workload = json.loads((ROOT/"examples/cache-workload.json").read_text())
    rng = random.Random(7)
    args.documents = [("A" if i < args.requests//2 else "C") if rng.random() < .8 else "B" for i in range(args.requests)]
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
              "platform": platform.platform(), "assets": config, "workload": workload,
              "settings": vars(args)|{"output_dir": str(output)}, "runs": []}
    order = [(trial, policy, rate) for trial in range(args.trials) for policy in ("least-inflight", "prefix-affinity") for rate in args.rates]
    random.Random(11).shuffle(order)
    result["condition_order_seed"] = 11
    try:
        for trial, policy, rate in order:
            result["runs"].append(run(policy, rate, trial, args, config, workload, output))
        result["experiment_completed"] = True
    except Exception as err:
        result["experiment_completed"] = False; result["error"] = str(err)
        raise
    finally:
        (output/"results.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
