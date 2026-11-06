"""Restart a real model process while independent arrivals continue through the gateway."""

import argparse
import concurrent.futures
import contextlib
import json
from pathlib import Path
import socket
import subprocess
import time

from bench_load import send_request
from check_real_health import first_content, wait_states
from check_real_inference import ROOT, get_json, open_stream
from check_real_pool import service


def collect_interruption(stream, connection, response):
    try:
        completed = any(event is None for event in stream)
        return {"completed": completed, "error": None if completed else "EOF without DONE"}
    except Exception as err:
        return {"completed": False, "error": type(err).__name__+": "+str(err)}
    finally:
        response.close(); connection.close()


def arrivals(start, count, rate, payload):
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as executor:
        for i in range(count):
            due = start+i/rate
            time.sleep(max(0, due-time.perf_counter()))
            request = dict(payload, max_tokens=16, ignore_eos=True, stream_options={"include_usage": True})
            futures.append(executor.submit(send_request, i, "restart-test", due, request, 2000, 3000))
        samples = [future.result() for future in futures]
    for i, sample in enumerate(samples):
        sample["scheduled_seconds"] = i/rate
    return samples


def run(policy, trial, args, config, output):
    label = f"{trial}-{policy}"
    result = {"policy": policy, "trial": trial, "commands": [], "events": []}
    engine = ROOT/".tools"/("llama-"+config["release"])/"llama-server.exe"
    payload = json.loads((ROOT/"examples/real-stream.json").read_text())
    with contextlib.ExitStack() as stack:
        processes = []
        for i in range(2):
            command = [str(engine), "-m", str(ROOT/".tools/models"/config["model_file"]), "--host", "127.0.0.1",
                       "--port", str(18081+i), "--alias", "smollm2-135m", "-c", "2048", "-np", "1", "-t", "2",
                       "-ngl", "0", "--slots", "--no-ui", "--cache-prompt", "--cache-ram", "0"]
            result["commands"].append(command)
            processes.append(stack.enter_context(service(command, f"http://127.0.0.1:{18081+i}/health", output/f"{label}-worker{i}.log")))
        command = [str(ROOT/"bin/gateway.exe"), "-listen", "127.0.0.1:18080", "-admin-listen", "127.0.0.1:18090",
                   "-backends", "http://127.0.0.1:18081,http://127.0.0.1:18082", "-policy", policy, "-max-inflight", "1",
                   "-request-timeout", "60s", "-health-interval", "100ms", "-health-timeout", "500ms"]
        result["commands"].append(command)
        stack.enter_context(service(command, "http://127.0.0.1:18080/readyz", output/f"{label}-gateway.log"))
        wait_states(lambda s: all(w["healthy"] for w in s))
        conn, response = open_stream("http://127.0.0.1:18080", dict(payload, max_tokens=1024, ignore_eos=True), {"X-Prefix-Key": "restart-test"})
        try:
            stream = first_content(response)
            victim = int(response.getheader("X-Inference-Worker"))
        except BaseException:
            response.close(); conn.close(); raise
        start = time.perf_counter()+.1
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as background:
            control = background.submit(collect_interruption, stream, conn, response)
            traffic = background.submit(arrivals, start, args.requests, args.rate, payload)
            try:
                time.sleep(max(0, start+2-time.perf_counter()))
                result["slot_before_kill"] = get_json(f"http://127.0.0.1:{18081+victim}", "/slots")
                if not result["slot_before_kill"][0]["is_processing"] or control.done():
                    raise RuntimeError("Target must still be generating when killed")
                result["victim"] = victim
                processes[victim].terminate(); processes[victim].wait(timeout=5)
                result["events"].append({"event": "killed", "seconds": time.perf_counter()-start})
                result["unhealthy_state"] = wait_states(lambda s: not s[victim]["healthy"] and s[1-victim]["healthy"])
                loss = time.perf_counter()-start
                result["events"].append({"event": "unhealthy_observed", "seconds": loss})
                result["interrupted_control_stream"] = control.result(timeout=5)
                if result["interrupted_control_stream"]["completed"]:
                    raise RuntimeError("Killed generation appeared complete")
                time.sleep(2)
                stack.enter_context(service(result["commands"][victim], f"http://127.0.0.1:{18081+victim}/health", output/f"{label}-restarted.log"))
                result["recovered_state"] = wait_states(lambda s: all(w["healthy"] for w in s))
                recovered = time.perf_counter()-start
                result["events"].append({"event": "recovered_observed", "seconds": recovered})
                result["samples"] = traffic.result(timeout=args.requests/args.rate+70)
            finally:
                response.close(); conn.close()
        samples = result["samples"]
        result.update(offered=len(samples), completed=sum(s["outcome"] == "completed" for s in samples),
                      rejected=sum(s["outcome"] == "rejected" for s in samples),
                      failed=sum(s["outcome"] in ("failed", "http_error") for s in samples))
        result["survivor_completed_during_loss"] = sum(s["outcome"] == "completed" and s["worker"] == str(1-victim)
                                                       and loss <= s["scheduled_seconds"] < recovered for s in samples)
        result["restarted_completed_after_recovery"] = sum(s["outcome"] == "completed" and s["worker"] == str(victim)
                                                          and s["scheduled_seconds"] >= recovered for s in samples)
        result["final_gateway_state"] = wait_states(lambda s: all(w["inflight"] == 0 and w["healthy"] for w in s))
        result["final_engine_slots"] = [get_json(f"http://127.0.0.1:{18081+i}", "/slots") for i in range(2)]
        assert result["completed"]+result["rejected"]+result["failed"] == args.requests
        if not result["survivor_completed_during_loss"] or not result["restarted_completed_after_recovery"]:
            raise RuntimeError("Did not observe service on survivor and recovered worker")
        if any(slot["is_processing"] for slots in result["final_engine_slots"] for slot in slots):
            raise RuntimeError("Engine remained busy after traffic drained")
        result["passed"] = True
        (output/f"{label}.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")
        print("PASS",label,"completed",result["completed"],"rejected",result["rejected"],"failed",result["failed"],
              "survivor",result["survivor_completed_during_loss"],"recovered",result["restarted_completed_after_recovery"],flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--requests", type=int, default=32)
    parser.add_argument("--rate", type=float, default=2)
    parser.add_argument("--output-dir", type=Path, default=ROOT/"artifacts/restart-load")
    args = parser.parse_args()
    if not 16 <= args.requests <= 64 or args.trials < 1 or not .5 <= args.rate <= 4 or args.requests/args.rate < 12:
        parser.error("require 16..64 requests, positive trials, rate .5..4, arrival window >= 12 seconds")
    for port in (18080, 18081, 18082, 18090):
        with socket.socket() as probe: probe.bind(("127.0.0.1", port))
    output = args.output_dir.resolve(); output.mkdir(parents=True, exist_ok=True)
    config = json.loads((ROOT/"config/llama-cpu.json").read_text())
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
              "assets": config, "settings": vars(args)|{"output_dir": str(output)}, "runs": []}
    try:
        for trial in range(args.trials):
            for policy in ("least-inflight", "prefix-affinity"):
                result["runs"].append(run(policy, trial, args, config, output))
        result["passed"] = True
    except Exception as err:
        result.update(passed=False, error=str(err)); raise
    finally:
        (output/"results.json").write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
