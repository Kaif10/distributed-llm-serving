"""Exercise mutual-TLS peers, roles and restart; optional Linux network namespaces.

Certificates are disposable fixtures, not production CA provisioning. Namespace
mode requires root/ip/tc and creates only uniquely named, owned network devices.
Separate namespaces have independent network stacks but share a host and kernel.
"""
import argparse
import contextlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary",type=Path)
    parser.add_argument("--output",required=True,type=Path)
    parser.add_argument("--network-namespaces",action="store_true")
    parser.add_argument("--gateway",type=Path,help="also verify the real gateway's HTTPS controller polling")
    args=parser.parse_args();binary=args.binary.resolve();output=args.output.resolve()
    output.parent.mkdir(parents=True,exist_ok=True)
    openssl=shutil.which("openssl")
    if not openssl and os.name=="nt":openssl=r"C:\Program Files\Git\usr\bin\openssl.exe"
    if not openssl:raise RuntimeError("OpenSSL required for disposable test certificates")
    report={"source_commit":subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
            "working_tree":subprocess.check_output(["git","status","--porcelain"],cwd=ROOT,text=True),
            "scope":"one host; native processes; mutual TLS; "+("isolated network namespaces, delay and bidirectional partition" if args.network_namespaces else "loopback sockets"),
            "passed":False,"checks":[]}
    ids=list("abc");processes={};files=[];namespaces=[];devices=[];gateway_process=None

    def command(*argv):return subprocess.check_output(list(argv),stderr=subprocess.STDOUT,text=True)
    def wait(check,label):
        deadline=time.monotonic()+25
        while time.monotonic()<deadline:
            try:
                value=check()
                if value:return value
            except (OSError,ValueError,KeyError) as error:report["last_wait_error"]=repr(error)
            time.sleep(.05)
        raise RuntimeError("timeout: "+label)
    try:
        with tempfile.TemporaryDirectory(prefix="secure-peers-") as directory:
            root=Path(directory)
            try:
                if args.network_namespaces:
                    if os.name!="posix" or os.geteuid()!=0:raise RuntimeError("namespace mode requires Linux root")
                    suffix=secrets.token_hex(3);bridge="fb"+suffix
                    # Refuse overlap with any existing IPv4 route except default.
                    routes=json.loads(command("ip","-j","-4","route","show"))
                    existing=[ipaddress.ip_network(r["dst"],strict=False) for r in routes if r.get("dst") not in (None,"default")]
                    available=[i for i in range(1,255) if not any(ipaddress.ip_network(f"198.18.{i}.0/24").overlaps(n) for n in existing)]
                    if not available:raise RuntimeError("no free test subnet")
                    subnet=secrets.choice(available)
                    command("ip","link","add",bridge,"type","bridge");devices.append(bridge)
                    command("ip","addr","add",f"198.18.{subnet}.1/24","dev",bridge)
                    command("ip","link","set",bridge,"up")
                    hosts={n:f"198.18.{subnet}.{11+i}" for i,n in enumerate(ids)}
                    links={}
                    for n in ids:
                        namespace="fleet-"+suffix+"-"+n;host="fh"+suffix+n;inside="fn"+suffix+n
                        command("ip","netns","add",namespace);namespaces.append(namespace)
                        command("ip","link","add",host,"type","veth","peer","name",inside);devices.append(host)
                        command("ip","link","set",inside,"netns",namespace)
                        command("ip","link","set",host,"master",bridge);command("ip","link","set",host,"up")
                        command("ip","-n",namespace,"link","set","lo","up")
                        command("ip","-n",namespace,"addr","add",hosts[n]+"/24","dev",inside)
                        command("ip","-n",namespace,"link","set",inside,"up")
                        command("tc","qdisc","add","dev",host,"root","netem","delay","15ms")
                        command("ip","netns","exec",namespace,"tc","qdisc","add","dev",inside,"root","netem","delay","15ms")
                        links[n]=(namespace,host,inside)
                    peerports={n:19201 for n in ids};clientports={n:19301 for n in ids}
                else:
                    hosts={n:"127.0.0.1" for n in ids};sockets=[socket.socket() for _ in range(6)]
                    for s in sockets:s.bind(("127.0.0.1",0))
                    ports=[s.getsockname()[1] for s in sockets]
                    for s in sockets:s.close()
                    peerports=dict(zip(ids,ports[:3]));clientports=dict(zip(ids,ports[3:]))
                command(openssl,"req","-x509","-newkey","ec","-pkeyopt","ec_paramgen_curve:P-256","-nodes","-days","1",
                        "-addext","basicConstraints=critical,CA:TRUE","-addext","keyUsage=critical,keyCertSign,cRLSign",
                        "-subj","/CN=disposable-fleet-test","-keyout",str(root/"ca.key"),"-out",str(root/"ca.pem"))
                def certificate(name,role,identity,host="127.0.0.1"):
                    command(openssl,"req","-new","-newkey","ec","-pkeyopt","ec_paramgen_curve:P-256","-nodes",
                            "-subj","/CN="+name,"-keyout",str(root/(name+".key")),"-out",str(root/(name+".csr")))
                    extension=root/(name+".cnf")
                    extension.write_text(f"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=IP:{host},URI:spiffe://distributed-llm-serving/{role}/{identity}\n")
                    command(openssl,"x509","-req","-in",str(root/(name+".csr")),"-CA",str(root/"ca.pem"),"-CAkey",str(root/"ca.key"),
                            "-CAcreateserial","-days","1","-extfile",str(extension),"-out",str(root/(name+".pem")))
                for n in ids:certificate(n,"peer",n,hosts[n])
                certificate("operator","admin","operator");certificate("gateway","reader","gateway")
                # A CA the server does not trust must not authenticate.
                command(openssl,"req","-x509","-newkey","ec","-pkeyopt","ec_paramgen_curve:P-256","-nodes","-days","1",
                        "-subj","/CN=untrusted","-keyout",str(root/"rogue.key"),"-out",str(root/"rogue.pem"))
                def opener(identity):
                    ctx=ssl.create_default_context(cafile=str(root/"ca.pem"))
                    ctx.verify_flags |= ssl.VERIFY_X509_STRICT
                    if identity:ctx.load_cert_chain(str(root/(identity+".pem")),str(root/(identity+".key")))
                    return urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=ctx))
                clients={n:opener(n) for n in ["operator","gateway","rogue","a"]};clients[None]=opener(None)
                def rpc(node,method,path="/config",value=None,identity="operator",peer=False):
                    port=(peerports if peer else clientports)[node]
                    req=urllib.request.Request(f"https://{hosts[node]}:{port}"+path,data=None if value is None else json.dumps(value).encode(),
                                               method=method,headers={"Content-Type":"application/json"})
                    try:
                        with clients[identity].open(req,timeout=7) as response:
                            raw=response.read();return response.status,json.loads(raw) if raw else None
                    except urllib.error.HTTPError as error:
                        code=error.code;error.close();return code,None
                def status(n):
                    code,state=rpc(n,"GET","/status");return state if code==200 else None
                def leader(eligible=None):
                    found=[n for n in (eligible or list(processes)) if status(n)["node"]["Role"]=="leader"]
                    return found[0] if len(found)==1 else None
                spec=",".join(f"{n}=https://{hosts[n]}:{peerports[n]}" for n in ids)
                def start(n):
                    log=(output.parent/("tls-"+n+".log")).open("ab");files.append(log)
                    argv=[str(binary),"-configuration","-id",n,"-peers",spec,"-client-listen",f"{hosts[n]}:{clientports[n]}",
                          "-retained-dir",str(root/("data-"+n)),"-retained-group","secure-test",
                          "-tls-cert",str(root/(n+".pem")),"-tls-key",str(root/(n+".key")),"-tls-ca",str(root/"ca.pem")]
                    if args.network_namespaces:argv=["ip","netns","exec",links[n][0],*argv]
                    processes[n]=subprocess.Popen(argv,stdout=log,stderr=log)
                    wait(lambda:status(n),"secure listener "+n)
                def stop(n):
                    p=processes.pop(n)
                    if p.poll() is None:p.kill()
                    p.wait(timeout=8)
                for n in ids:start(n)
                active=wait(leader,"TLS election")
                first={"request_id":"tls-first","expected_revision":0,"workers":[]}
                assert rpc(active,"PUT",value=first)==(200,{"revision":1,"workers":[]})
                for _ in range(140):assert rpc(active,"PUT",value=first)[0]==200
                assert rpc(active,"GET",identity="gateway")== (200,{"revision":1,"workers":[]})
                assert rpc(active,"PUT",value=first,identity="gateway")[0]==403
                for identity in (None,"rogue"):
                    try:rpc(active,"GET",identity=identity)
                    except (OSError,urllib.error.URLError):pass
                    else:raise AssertionError("untrusted/missing client certificate accepted")
                target="c";before=status(target)["node"]["Term"]
                forged={"From":"b","To":target,"Term":before+100,"Type":"vote-request"}
                assert rpc(target,"POST","/raft/message",forged,identity="a",peer=True)[0]==403
                assert status(target)["node"]["Term"]<before+100
                assert rpc(target,"POST","/raft/message",forged,identity="operator",peer=True)[0]==403
                report["checks"] += ["replication and automatic retention over mTLS","reader can read but cannot write",
                                      "missing and untrusted client certificates rejected","peer certificate cannot impersonate another member","admin cannot send peer messages"]
                if args.gateway:
                    sockets=[socket.socket(),socket.socket()]
                    for s in sockets:s.bind(("127.0.0.1",0))
                    gateway_port,admin_port=[s.getsockname()[1] for s in sockets]
                    for s in sockets:s.close()
                    log=(output.parent/"tls-gateway.log").open("ab");files.append(log)
                    argv=[str(args.gateway.resolve()),"-listen",f"127.0.0.1:{gateway_port}","-admin-listen",f"127.0.0.1:{admin_port}",
                          "-controllers",",".join(f"https://{hosts[n]}:{clientports[n]}" for n in ids),
                          "-controller-tls-cert",str(root/"gateway.pem"),"-controller-tls-key",str(root/"gateway.key"),
                          "-controller-tls-ca",str(root/"ca.pem"),"-config-timeout","2s","-config-max-age","10s"]
                    gateway_process=subprocess.Popen(argv,stdout=log,stderr=log)
                    def gateway_fresh():
                        with urllib.request.urlopen(f"http://127.0.0.1:{admin_port}/config",timeout=2) as response:state=json.load(response)
                        return state if state["applied_revision"]==1 and state["fresh"] else None
                    report["gateway_poll"]=wait(gateway_fresh,"gateway confirms HTTPS controller state")
                    report["checks"].append("gateway polls controllers using reader credentials and confirms revision 1")
                if args.network_namespaces:
                    namespace,host,inside=links[active]
                    command("tc","qdisc","change","dev",host,"root","netem","loss","100%")
                    command("ip","netns","exec",namespace,"tc","qdisc","change","dev",inside,"root","netem","loss","100%")
                    try:
                        replacement=wait(lambda:leader([n for n in ids if n!=active]),"majority elects under partition")
                        assert rpc(replacement,"PUT",value={"request_id":"partition","expected_revision":1,"workers":[]})[0]==200
                    finally:
                        command("tc","qdisc","change","dev",host,"root","netem","delay","15ms")
                        command("ip","netns","exec",namespace,"tc","qdisc","change","dev",inside,"root","netem","delay","15ms")
                    wait(lambda:status(active)["configuration"]["revision"]==2,"partitioned member catches up")
                    report["checks"].append("15ms each-way interface delay and bidirectional 100% peer partition; majority write and heal")
                active=wait(leader,"leader before kill");stop(active)
                replacement=wait(leader,"TLS failover")
                assert rpc(replacement,"PUT",value=first)==(200,{"revision":1,"workers":[]})
                start(active)
                for n in list(processes):stop(n)
                for n in ids:start(n)
                active=wait(leader,"TLS full restart")
                assert rpc(active,"PUT",value=first)==(200,{"revision":1,"workers":[]})
                report["statuses"]={n:status(n) for n in ids}
                report["checks"].append("leader failure and full restart preserve original successful result")
                report["passed"]=True
                print("PASS: authenticated three-peer replication, role rejection, retention, leader loss and restart",flush=True)
            finally:
                if gateway_process:
                    if gateway_process.poll() is None:gateway_process.kill()
                    gateway_process.wait(timeout=8)
                for p in processes.values():
                    if p.poll() is None:p.kill()
                    p.wait(timeout=8)
                for f in files:f.close()
                for namespace in reversed(namespaces):subprocess.run(["ip","netns","del",namespace],capture_output=True)
                for device in reversed(devices):subprocess.run(["ip","link","del",device],capture_output=True)
    except Exception as error:
        report["error"]=repr(error);raise
    finally:
        output.write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")


if __name__=="__main__":main()
