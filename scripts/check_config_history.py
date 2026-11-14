"""Check client-visible CAS/read/retry histories over native peers under faults."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import itertools
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

from check_retained_controller import request
from config_history import check_history

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary",type=Path)
    parser.add_argument("--output",required=True,type=Path)
    args=parser.parse_args();binary=args.binary.resolve();output=args.output.resolve()
    output.parent.mkdir(parents=True,exist_ok=True)
    ids=list("abc");reservations=[socket.socket() for _ in range(9)]
    for sock in reservations:sock.bind(("127.0.0.1",0))
    ports=[s.getsockname()[1] for s in reservations]
    for sock in reservations:sock.close()
    backend=dict(zip(ids,ports[:3]));clients=dict(zip(ids,ports[3:6]));advertised=dict(zip(ids,ports[6:]))
    isolated=set();lock=threading.Lock();history=[];processes={};files=[];relays=[];threads=[];operation_ids=itertools.count()
    report={"source_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
            "working_tree":subprocess.check_output(["git","status","--porcelain"],cwd=ROOT,text=True),
            "scope":"finite config API histories; three native peers and real HTTP relays on one host; no inference",
            "history":history,"events":[],"passed":False}

    def handler(destination):
        class Relay(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                data=self.rfile.read(int(self.headers.get("Content-Length","0")))
                source=json.loads(data)["From"]
                with lock:blocked=source in isolated or destination in isolated
                code=503
                if not blocked:
                    try:
                        q=urllib.request.Request(f"http://127.0.0.1:{backend[destination]}/raft/message",data=data)
                        with urllib.request.urlopen(q,timeout=1) as r:code=r.status
                    except urllib.error.HTTPError as e:code=e.code;e.close()
                    except OSError:pass
                self.send_response(code);self.send_header("Content-Length","0");self.end_headers()
        return Relay

    def rpc(node,method,path,value=None):return request(clients[node],method,path,value)
    def tracked(node,method="GET",value=None):
        with lock:operation_id=next(operation_ids)
        op={"id":f"op-{operation_id}","node":node,"method":method,"path":"/config","request":copy.deepcopy(value),"call_ns":time.monotonic_ns()}
        try:op["status"],op["response"]=rpc(node,method,"/config",value)
        except OSError as e:op["status"],op["response"]=None,repr(e)
        op["return_ns"]=time.monotonic_ns()
        with lock:history.append(op)
        return op["status"],op["response"]
    def wait(check,label):
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            try:
                value=check()
                if value:return value
            except (OSError,KeyError,ValueError):pass
            time.sleep(.03)
        raise RuntimeError("timeout: "+label)
    def status(node):
        code,state=rpc(node,"GET","/status");assert code==200;return state
    def leader(eligible=None):
        found=[n for n in (eligible or list(processes)) if processes[n].poll() is None and status(n)["node"]["Role"]=="leader"]
        return found[0] if len(found)==1 else None
    def stop(node):
        p=processes.pop(node)
        if p.poll() is None:p.kill()
        p.wait(timeout=8)
    try:
        with tempfile.TemporaryDirectory(prefix="api-history-") as directory:
            root=Path(directory);peers=",".join(f"{n}=http://127.0.0.1:{advertised[n]}" for n in ids)
            def start(node):
                log=(root/(node+".log")).open("ab");files.append(log)
                command=[str(binary),"-configuration","-id",node,"-peers",peers,"-peer-listen",f"127.0.0.1:{backend[node]}",
                         "-client-listen",f"127.0.0.1:{clients[node]}","-retained-dir",str(root/node),"-retained-group","history"]
                processes[node]=subprocess.Popen(command,stdout=log,stderr=log)
                wait(lambda:status(node),"listener "+node)
            try:
                for node in ids:
                    relay=ThreadingHTTPServer(("127.0.0.1",advertised[node]),handler(node));relays.append(relay)
                    thread=threading.Thread(target=relay.serve_forever,daemon=True);thread.start();threads.append(thread)
                for node in ids:start(node)
                initial=wait(leader,"initial leader")
                first={"request_id":"original","expected_revision":0,"workers":[]}
                assert tracked(initial,"PUT",first)==(200,{"revision":1,"workers":[]})
                # Application-neutral retries exercise automatic retention before
                # the checked concurrent history. They add no abstract effects.
                for _ in range(1100):assert rpc(initial,"PUT","/config",first)==(200,{"revision":1,"workers":[]})
                report["events"].append({"healthy_retries":1100,"statuses":{n:status(n)["retention"] for n in ids}})
                with ThreadPoolExecutor(max_workers=3) as pool:
                    a={"request_id":"cas-a","expected_revision":1,"workers":[]}
                    b={"request_id":"cas-b","expected_revision":1,"workers":[{"id":"worker","origin":"http://127.0.0.1:20000","draining":True}]}
                    futures=[pool.submit(tracked,initial,"PUT",a),pool.submit(tracked,initial,"PUT",b),pool.submit(tracked,initial)]
                    outcomes=[f.result() for f in futures]
                assert sorted(x[0] for x in outcomes[:2])==[200,409]
                assert tracked(initial,"PUT",first)==(200,{"revision":1,"workers":[]})
                _,current=tracked(initial)
                with lock:isolated.add(initial)
                replacement=wait(lambda:leader([n for n in ids if n!=initial]),"majority leader")
                stale={"request_id":"isolated-write","expected_revision":current["revision"],"workers":[]}
                good={"request_id":"majority-write","expected_revision":current["revision"],"workers":[]}
                with ThreadPoolExecutor(max_workers=3) as pool:
                    futures=[pool.submit(tracked,initial),pool.submit(tracked,initial,"PUT",stale),pool.submit(tracked,replacement,"PUT",good)]
                    results=[f.result() for f in futures]
                assert results[0][0]!=200 and results[1][0]!=200 and results[2][0]==200
                report["events"].append({"isolated_old_leader":initial,"majority_leader":replacement})
                with lock:isolated.clear()
                active=wait(leader,"healed leader");tracked(active)
                assert rpc(active,"POST","/retain")[0]==200
                stop(active)
                successor=wait(leader,"leader after process kill");tracked(successor)
                start(active)
                wait(lambda:status(active)["configuration"]["revision"]==3,"killed peer catches up")
                for node in list(processes):stop(node)
                for node in ids:start(node)
                active=wait(leader,"leader after full restart")
                code,current=tracked(active);assert code==200 and current["revision"]==3
                final={"request_id":"final","expected_revision":3,"workers":[]}
                with ThreadPoolExecutor(max_workers=3) as pool:
                    futures=[pool.submit(tracked,active,"PUT",first),pool.submit(tracked,active,"PUT",final),pool.submit(tracked,active)]
                    for future in futures:future.result()
                report["check"]=check_history(history)
                assert report["check"]["linearizable"],"client-visible history has no legal sequential explanation"
                report["passed"]=True
                print("PASS: client config history is linearizable across concurrent CAS, partition, retention, leader loss, and full restart",flush=True)
            finally:
                for node in list(processes):stop(node)
                for log in files:log.close()
                for relay in relays:relay.shutdown();relay.server_close()
                for thread in threads:thread.join(timeout=3)
    except Exception as error:
        report["error"]=repr(error);raise
    finally:
        output.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")


if __name__=="__main__":main()
