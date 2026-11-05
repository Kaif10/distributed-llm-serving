"""Measure real prompt-cache reuse and existing routing baselines with fresh engines."""

import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import platform
import random
import socket
import statistics
import subprocess

from check_real_inference import ROOT, get_json, measure
from check_real_pool import service


def request_for(workload, document, position, cache):
    return {"model": "smollm2-135m", "messages": [
        {"role": "system", "content": workload["documents"][document]},
        {"role": "user", "content": workload["question_template"].format(record=position+1)}],
        "stream": True, "max_tokens": 16, "ignore_eos": True, "temperature": 0,
        "seed": 7, "cache_prompt": cache, "stream_options": {"include_usage": True}}


def run_condition(condition, trial, config, workload, output):
    cache = condition != "direct-no-cache"
    count = 1 if condition.startswith("direct") else 2
    origins = [f"http://127.0.0.1:{18081+i}" for i in range(count)]
    result = {"condition": condition, "trial": trial, "commands": [], "samples": [], "failures": 0}
    engine = ROOT / ".tools" / ("llama-"+config["release"]) / "llama-server.exe"
    model = ROOT / ".tools/models" / config["model_file"]
    with contextlib.ExitStack() as stack:
        for i, origin in enumerate(origins):
            command = [str(engine), "-m", str(model), "--host", "127.0.0.1", "--port", str(18081+i),
                       "--alias", "smollm2-135m", "-c", "2048", "-np", "1", "-t", "2", "-ngl", "0",
                       "--slots", "--no-ui", "--cache-ram", "0", "--no-cache-idle-slots",
                       "--cache-prompt" if cache else "--no-cache-prompt"]
            result["commands"].append(command)
            stack.enter_context(service(command, origin+"/health", output/f"{trial}-{condition}-worker{i}.log"))
        destination = origins[0]
        if count == 2:
            destination = "http://127.0.0.1:18080"
            command = [str(ROOT/"bin/gateway.exe"), "-listen", "127.0.0.1:18080", "-backends", ",".join(origins),
                       "-policy", condition, "-max-inflight", "1", "-request-timeout", "60s"]
            result["commands"].append(command)
            stack.enter_context(service(command, destination+"/readyz", output/f"{trial}-{condition}-gateway.log"))
        # Warm model execution with an unrelated prompt. Target document prefixes
        # are still cold, although common chat-template tokens can remain cached.
        warmup = {"model": "smollm2-135m", "messages": [{"role": "user", "content": "Say hello."}],
                  "stream": True, "max_tokens": 16, "ignore_eos": True, "temperature": 0, "cache_prompt": False}
        for origin in origins:
            measure(origin, warmup)
        result["warmup_per_engine"] = warmup
        for position, document in enumerate(workload["sequence"]):
            before = [get_json(origin, "/slots")[0].get("id_task") for origin in origins]
            payload = request_for(workload, document, position, cache)
            try:
                sample = measure(destination, payload)
                usage = sample["usage"]
                if usage is None or usage["completion_tokens"] != 16:
                    raise RuntimeError("Expected measured usage and 16 generated tokens")
                cached = usage["prompt_tokens_details"]["cached_tokens"]
                if not 0 <= cached <= usage["prompt_tokens"]:
                    raise RuntimeError("Invalid cached-token accounting")
                after = [get_json(origin, "/slots")[0].get("id_task") for origin in origins]
                touched = [i for i, (a, b) in enumerate(zip(before, after)) if a != b]
                if len(touched) != 1:
                    raise RuntimeError("Cannot attribute request to exactly one real engine")
                sample.update(document=document, position=position, worker=touched[0], cached_tokens=cached)
                result["samples"].append(sample)
                (output/f"{trial}-{condition}-samples.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")
            except Exception as err:
                result["failures"] += 1
                result["error"] = str(err)
                (output/f"failed-{trial}-{condition}.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")
                raise
        samples = result["samples"]
        if not cache and any(s["cached_tokens"] != 0 for s in samples):
            raise RuntimeError("Disabled-cache control unexpectedly reused tokens")
        if condition == "direct-cache":
            if samples[1]["cached_tokens"] <= samples[1]["usage"]["prompt_tokens"] // 2:
                raise RuntimeError("Repeated document did not reuse most of its prefix")
            if samples[2]["cached_tokens"] >= samples[2]["usage"]["prompt_tokens"] // 2:
                raise RuntimeError("Unrelated document unexpectedly reused most of its prefix")
        result["cached_token_fraction"] = sum(s["cached_tokens"] for s in samples) / sum(s["usage"]["prompt_tokens"] for s in samples)
        result["median_ttft_ms"] = statistics.median(s["ttft_ms"] for s in samples)
        print(trial, condition, "cached fraction", round(result["cached_token_fraction"], 3),
              "median TTFT", result["median_ttft_ms"], "ms", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=ROOT/"artifacts/cache-baseline")
    parser.add_argument("--conditions", nargs="+", choices=["direct-cache", "direct-no-cache", "round-robin", "least-inflight"],
                        default=["direct-cache", "direct-no-cache", "round-robin", "least-inflight"])
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("trials must be positive")
    for port in (18080, 18081, 18082):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    config = json.loads((ROOT/"config/llama-cpu.json").read_text())
    workload_path = ROOT/"examples/cache-workload.json"
    workload = json.loads(workload_path.read_text())
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
              "platform": platform.platform(), "assets": config, "workload": workload,
              "workload_sha256": hashlib.sha256(workload_path.read_bytes()).hexdigest(),
              "order_seed": 7, "trials": args.trials, "runs": []}
    rng = random.Random(7)
    try:
        for trial in range(args.trials):
            conditions = list(args.conditions)
            rng.shuffle(conditions)
            for condition in conditions:
                result["runs"].append(run_condition(condition, trial, config, workload, output))
        result["passed"] = True
    except Exception as err:
        result["passed"] = False
        result["error"] = str(err)
        raise
    finally:
        (output/"results.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
