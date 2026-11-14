"""Two gateways share one worker-side admission process and a real llama.cpp engine.

The worker-side process reuses the existing gateway binary with one backend. All
frontends must target it; bypassing it or running two guards for one engine defeats
the shared bound. This is a single-host mechanism check, not multi-host validation.
"""
import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.request

from check_real_inference import ROOT, events, get_json, measure, open_stream
from check_real_pool import service


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    assets = json.loads((ROOT/"config/llama-cpu.json").read_text(encoding="utf-8"))
    engine = ROOT/".tools"/("llama-"+assets["release"])/"llama-server.exe"
    model = ROOT/".tools/models"/assets["model_file"]
    assert hashlib.sha256(model.read_bytes()).hexdigest() == assets["model_sha256"]
    reservations = [socket.socket() for _ in range(7)]
    for sock in reservations:
        sock.bind(("127.0.0.1", 0))
    ports = [sock.getsockname()[1] for sock in reservations]
    for sock in reservations:
        sock.close()
    origins = [f"http://127.0.0.1:{port}" for port in ports]
    engine_origin, guard, guard_admin, a, a_admin, b, b_admin = origins
    payload = json.loads((ROOT/"examples/real-stream.json").read_text(encoding="utf-8"))
    result = {"source_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
              "working_tree":subprocess.check_output(["git","status","--porcelain"],cwd=ROOT,text=True),
              "scope":"two frontend gateways, one shared worker-side gate, one real CPU model; one host",
              "assets":assets,"commands":[],"passed":False}

    def wait(check, label):
        deadline = time.monotonic()+8
        while time.monotonic()<deadline:
            value = check()
            if value:
                return value
            time.sleep(.02)
        raise RuntimeError("timeout: "+label)

    def start(stack, command, health, name):
        result["commands"].append(command)
        return stack.enter_context(service(command, health, output.parent/(name+".log")))

    try:
        with contextlib.ExitStack() as stack:
            start(stack,[str(engine),"-m",str(model),"--host","127.0.0.1","--port",str(ports[0]),
                         "--alias","smollm2-135m","-c","2048","-np","1","-t","2","-ngl","0","--no-ui","--slots"],engine_origin+"/health","engine")
            gateway = str(ROOT/"bin/gateway.exe")
            start(stack,[gateway,"-listen",f"127.0.0.1:{ports[1]}","-admin-listen",f"127.0.0.1:{ports[2]}",
                         "-backend",engine_origin,"-max-inflight","1","-request-timeout","60s"],guard+"/readyz","worker-gate")
            for index, origin, admin in [(3,a,a_admin),(5,b,b_admin)]:
                start(stack,[gateway,"-listen",f"127.0.0.1:{ports[index]}","-admin-listen",f"127.0.0.1:{ports[index+1]}",
                             "-backend",guard,"-health-path","/readyz","-max-inflight","1","-request-timeout","60s"],origin+"/readyz","frontend-"+str(index))
            connection, response = open_stream(a,dict(payload,max_tokens=1024,ignore_eos=True))
            try:
                for event in events(response):
                    if event is None:
                        raise RuntimeError("generation ended before contention check")
                    if any(c.get("delta",{}).get("content") for c in event.get("choices",[])):
                        break
                result["before_contention"] = {"frontend_a":get_json(a_admin,"/workers"),"frontend_b":get_json(b_admin,"/workers"),
                                               "worker_gate":get_json(guard_admin,"/workers"),"engine_slots":get_json(engine_origin,"/slots")}
                assert result["before_contention"]["frontend_a"][0]["inflight"] == 1
                assert result["before_contention"]["frontend_b"][0]["inflight"] == 0
                assert result["before_contention"]["worker_gate"][0]["inflight"] == 1
                req = urllib.request.Request(b+"/v1/chat/completions",data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"})
                try:
                    with urllib.request.urlopen(req,timeout=5):
                        raise AssertionError("second gateway bypassed shared admission")
                except urllib.error.HTTPError as error:
                    result["second_gateway_rejected"] = {"status":error.code,"retry_after":error.headers.get("Retry-After"),"body":error.read().decode()}
                    error.close()
                assert result["second_gateway_rejected"]["status"] == 503
                assert result["second_gateway_rejected"]["retry_after"] == "1"
                assert get_json(guard_admin,"/workers")[0]["inflight"] == 1
                assert get_json(engine_origin,"/slots")[0]["is_processing"]
            finally:
                response.close()
                connection.close()
            wait(lambda:not get_json(guard_admin,"/workers")[0]["inflight"],"shared admission release")
            wait(lambda:not get_json(engine_origin,"/slots")[0]["is_processing"],"real engine cancellation")
            wait(lambda:not get_json(b_admin,"/workers")[0]["inflight"],"frontend rejection release")
            result["generation_through_second_gateway"] = measure(b,dict(payload,max_tokens=16))
            assert result["generation_through_second_gateway"]["done"]
            result["passed"] = True
            print("PASS: two gateways share worker admission; overload rejected, cancellation releases engine, second gateway generates",flush=True)
    except Exception as error:
        result["error"] = repr(error)
        raise
    finally:
        output.write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")


if __name__ == "__main__":
    main()
