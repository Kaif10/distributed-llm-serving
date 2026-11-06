"""Verify durable drain consumption and stale-view expiry with a real model stream."""

import contextlib
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from check_controller import put, update
from check_real_health import first_content
from check_real_inference import ROOT, events, get_json, measure, open_stream
from check_real_pool import service


def wait_config(predicate):
    deadline=time.monotonic()+8
    while time.monotonic()<deadline:
        state=get_json("http://127.0.0.1:18090","/config")
        if predicate(state): return state
        time.sleep(.02)
    raise RuntimeError("Configuration observation did not converge: "+repr(state))


def rejected(path, payload=None):
    request=urllib.request.Request("http://127.0.0.1:18080"+path,data=json.dumps(payload).encode() if payload else None,
                                   headers={"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(request,timeout=3): raise RuntimeError("Expected 503")
    except urllib.error.HTTPError as err:
        code=err.code; err.close()
        if code!=503: raise RuntimeError("Unexpected status "+str(code))


def main():
    output=ROOT/"artifacts/config-follow"; output.mkdir(parents=True,exist_ok=True)
    for port in [18080,18081,18090,19090]:
        with socket.socket() as probe: probe.bind(("127.0.0.1",port))
    assets=json.loads((ROOT/"config/llama-cpu.json").read_text())
    payload=json.loads((ROOT/"examples/real-stream.json").read_text())
    result={"source_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
            "working_tree_dirty":bool(subprocess.check_output(["git","status","--porcelain"],cwd=ROOT,text=True)),"assets":assets,"commands":[]}
    try:
        with tempfile.TemporaryDirectory(prefix="journal-",dir=output) as temporary, contextlib.ExitStack() as stack:
            controller=[str(ROOT/"bin/controller.exe"),"-listen","127.0.0.1:19090","-journal",str(Path(temporary)/"fleet.wal")]
            result["commands"].append(controller)
            process=stack.enter_context(service(controller,"http://127.0.0.1:19090/config",output/"controller.log"))
            assert put(update("initial",0,False))[0]==200
            engine=[str(ROOT/".tools"/("llama-"+assets["release"])/"llama-server.exe"),"-m",str(ROOT/".tools/models"/assets["model_file"]),
                    "--host","127.0.0.1","--port","18081","--alias","smollm2-135m","-c","2048","-np","1","-t","2","-ngl","0","--no-ui","--slots"]
            result["commands"].append(engine)
            stack.enter_context(service(engine,"http://127.0.0.1:18081/health",output/"engine.log"))
            gateway=[str(ROOT/"bin/gateway.exe"),"-listen","127.0.0.1:18080","-admin-listen","127.0.0.1:18090",
                     "-backend","http://127.0.0.1:18081","-controller","http://127.0.0.1:19090","-config-poll","100ms",
                     "-config-timeout","200ms","-config-max-age","2s","-request-timeout","60s"]
            result["commands"].append(gateway)
            stack.enter_context(service(gateway,"http://127.0.0.1:18080/readyz",output/"gateway.log"))
            result["initial_applied"]=wait_config(lambda s:s["applied_revision"]==1 and s["fresh"])
            conn,response=open_stream("http://127.0.0.1:18080",dict(payload,max_tokens=256,ignore_eos=True))
            try:
                stream=first_content(response)
                result["drain_ack"]=put(update("drain",1,True))[1]
                result["drain_applied"]=wait_config(lambda s:s["applied_revision"]==2 and s["fresh"])
                result["worker_while_draining"]=get_json("http://127.0.0.1:18090","/workers")
                assert result["worker_while_draining"][0]["draining"] and result["worker_while_draining"][0]["inflight"]==1
                rejected("/v1/chat/completions",payload)
                assert any(event is None for event in stream)
                result["drained_stream_completed"]=True
            finally: response.close();conn.close()
            assert put(update("resume",2,False))[0]==200
            wait_config(lambda s:s["applied_revision"]==3 and s["fresh"])
            conn,response=open_stream("http://127.0.0.1:18080",dict(payload,max_tokens=1024,ignore_eos=True))
            try:
                stream=first_content(response)
                process.kill();process.wait(timeout=5)
                result["expired"]=wait_config(lambda s:not s["fresh"])
                assert get_json("http://127.0.0.1:18090","/workers")[0]["inflight"]==1
                rejected("/readyz")
                assert any(event is None for event in stream)
                result["stream_survived_expiry"]=True
            finally: response.close();conn.close()
            # With the engine idle, reject because the configuration is stale, not full.
            deadline=time.monotonic()+3
            while get_json("http://127.0.0.1:18090","/workers")[0]["inflight"]:
                if time.monotonic()>deadline:raise RuntimeError("reservation leaked")
                time.sleep(.02)
            rejected("/v1/chat/completions",payload)
            stack.enter_context(service(controller,"http://127.0.0.1:19090/config",output/"controller-restarted.log"))
            result["reconfirmed"]=wait_config(lambda s:s["fresh"] and s["applied_revision"]==3)
            result["generation_after_recovery"]=measure("http://127.0.0.1:18080",dict(payload,max_tokens=16))
            result["passed"]=True
            print("PASS: durable drain applied without truncation; expiry blocks new work while a real stream finishes; same-revision restart restores service",flush=True)
    except Exception as err:
        result.update(passed=False,error=str(err));raise
    finally:
        (output/"results.json").write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")


if __name__=="__main__": main()
