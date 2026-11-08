"""Measure bounded controller storage/recovery with actual single-member peer processes.

Closed-loop administrative requests; no inference traffic or multi-host scaling claim.
The full profile measures entry and payload exhaustion plus paired recovery trials.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zlib

ROOT = Path(__file__).resolve().parents[1]


def request(port, path, method="GET", value=None):
    data = None if value is None else json.dumps(value).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            code, body = response.status, json.load(response)
    except urllib.error.HTTPError as error:
        code, body = error.code, error.read().decode("utf-8")
        error.close()
    return code, body, round((time.perf_counter()-started)*1000, 3)


def free_ports():
    with socket.socket() as peer, socket.socket() as client:
        peer.bind(("127.0.0.1", 0))
        client.bind(("127.0.0.1", 0))
        return peer.getsockname()[1], client.getsockname()[1]


def offline_log(path):
    """Inspect the last CRC-valid full image of the current raft-state-v2 journal.

    This read-only experiment inspector is not a replacement recovery implementation.
    """
    last, records = None, 0
    with path.open("rb") as stream:
        while header := stream.read(12):
            assert len(header) == 12, "incomplete journal header"
            length, checksum, header_checksum = struct.unpack("<III", header)
            assert 0 < length <= 8 << 20 and zlib.crc32(header[:8]) == header_checksum
            payload = stream.read(length)
            assert len(payload) == length and zlib.crc32(payload) == checksum, "invalid journal record"
            last = json.loads(payload)
            records += 1
    assert last and last["kind"] == "raft-state-v2", "unexpected journal format"
    return last["log"], records


def workers(count):
    # Desired configuration only: no workers are started for this control test.
    # Long stable IDs and distinct loopback origins exercise legal larger payloads.
    return [{"id": (f"worker-{i:03d}-" + "x"*110) if count > 1 else "worker-a",
             "origin": f"http://127.0.0.1:{20000+i}", "draining": False} for i in range(count)]


def update(index, fleet):
    return {"request_id": f"update-{index}", "expected_revision": index, "workers": fleet}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--go", default="go")
    parser.add_argument("--profile", choices=["full", "quick"], default="full")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="capacity-", dir=output.parent)).resolve()
    if root.parent != output.parent or not root.name.startswith("capacity-"):
        raise RuntimeError("unexpected experiment directory; refusing cleanup")
    processes, handles = [], []
    recovery_updates, trials = (16, 1) if args.profile == "quick" else (128, 3)
    result = {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "go_version": subprocess.check_output([args.go, "version", str(binary)], text=True).strip(),
              "command": [sys.executable, *sys.argv], "platform": platform.platform(),
              "python": platform.python_version(), "processor": os.environ.get("PROCESSOR_IDENTIFIER", platform.processor()),
              "logical_cpus": os.cpu_count(), "profile": args.profile,
              "scope": "real single-member Raft processes, local synced journals, closed-loop admin writes; no inference",
              "commands": [], "recovery": [], "limits": [], "experiment_directory": str(root)}

    class Peer:
        def __init__(self, directory):
            self.directory = directory
            directory.mkdir()
            self.journal = directory / "raft.wal"
            self.checkpoint = directory / "checkpoint.wal"

        def start(self):
            peer_port, self.port = free_ports()
            self.log_path = self.directory / "process.log"
            log = self.log_path.open("w", encoding="utf-8")
            handles.append(log)
            command = [str(binary), "-configuration", "-id", "a", "-peers", f"a=http://127.0.0.1:{peer_port}",
                       "-client-listen", f"127.0.0.1:{self.port}", "-journal", str(self.journal),
                       "-checkpoint-group", "capacity-check", "-checkpoint-journal", str(self.checkpoint)]
            result["commands"].append(command)
            self.started = time.perf_counter()
            self.process = subprocess.Popen(command, stdout=log, stderr=log,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            processes.append(self.process)

        def ready(self, expected_revision, allow_exit=False):
            deadline = time.monotonic()+30
            listener_ms, first_status = None, None
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    self.process.wait(timeout=5)
                    detail = {"fresh_read": False, "exit_code": self.process.returncode,
                              "listener_ready_ms": listener_ms, "initial_status": first_status,
                              "log": self.log_path.read_text(encoding="utf-8")}
                    assert allow_exit, detail
                    return detail
                try:
                    code, state, _ = request(self.port, "/status")
                    if code == 200 and listener_ms is None:
                        listener_ms = round((time.perf_counter()-self.started)*1000, 3)
                        first_status = {"term": state["node"]["Term"], "commit_index": state["commit_index"],
                                        "applied_index": state.get("applied_index", 0), "checkpoint": state["checkpoint"]}
                    code, snapshot, _ = request(self.port, "/config")
                    if code == 200:
                        assert snapshot["revision"] == expected_revision, snapshot
                        _, state, _ = request(self.port, "/status")
                        return {"fresh_read": True, "listener_ready_ms": listener_ms,
                                "fresh_read_ready_ms": round((time.perf_counter()-self.started)*1000, 3),
                                "initial_status": first_status, "state": state}
                    assert code == 503, (code, snapshot)
                except (OSError, ValueError):
                    pass
                time.sleep(.01)
            raise AssertionError("peer startup did not settle")

        def stop(self):
            if self.process.poll() is None:
                self.process.kill()
            self.process.wait(timeout=5)

    def seed(directory, count):
        peer = Peer(directory)
        peer.start()
        peer.ready(0)
        fleet = workers(1)
        for i in range(count):
            code, body, _ = request(peer.port, "/config", "PUT", update(i, fleet))
            assert code == 200 and body["revision"] == i+1, (code, body)
        code, capture, _ = request(peer.port, "/checkpoint", "POST")
        assert code == 200, (code, capture)
        peer.stop()
        return peer, capture

    try:
        template, selected = seed(root / "recovery-template", recovery_updates)
        pairs = {}
        for trial in range(trials):
            # Alternate order to avoid always giving one variant the later cache state.
            for mode in (["full", "checkpoint"] if trial % 2 == 0 else ["checkpoint", "full"]):
                peer = Peer(root / f"recovery-{trial}-{mode}")
                shutil.copyfile(template.journal, peer.journal)
                if mode == "checkpoint":
                    shutil.copyfile(template.checkpoint, peer.checkpoint)
                journal_bytes = peer.journal.stat().st_size
                peer.start()
                ready = peer.ready(recovery_updates)
                state = ready.pop("state")
                assert state["checkpoint"]["recovery_index"] == (selected["published"]["position"]["index"] if mode == "checkpoint" else 0)
                code, image, _ = request(peer.port, "/checkpoint", "POST")
                assert code == 200, (code, image)
                pairs[(trial, mode)] = image["published"]["sha256"]
                code, retry, _ = request(peer.port, "/config", "PUT", update(0, workers(1)))
                assert code == 200 and retry["revision"] == 1, (code, retry)
                peer.stop()
                result["recovery"].append({"trial": trial, "mode": mode, "updates": recovery_updates,
                    "journal_bytes_before": journal_bytes, **ready, "applied_revision": state["configuration"]["revision"],
                    "checkpoint_status": state["checkpoint"], "complete_image_sha256": image["published"]["sha256"],
                    "historical_retry_revision": retry["revision"]})
            assert pairs[(trial, "full")] == pairs[(trial, "checkpoint")], "recovery images differ"

        if args.profile == "full":
            for kind, fleet in [("entry-count", workers(1)), ("payload-bytes", workers(128))]:
                peer = Peer(root / kind)
                peer.start()
                peer.ready(0)
                writes, samples, successes = [], [], 0
                for i in range(1025):
                    code, body, latency = request(peer.port, "/config", "PUT", update(i, fleet))
                    writes.append({"attempt": i+1, "status": code, "latency_ms": latency})
                    if code == 503:
                        assert ("1024 entries" if kind == "entry-count" else "four MiB") in body, body
                        rejection = body
                        break
                    assert code == 200 and body["revision"] == i+1, (code, body)
                    successes += 1
                    if successes in (1, 64, 128, 256, 512, 1023):
                        samples.append({"acknowledged_updates": successes, "journal_bytes": peer.journal.stat().st_size})
                else:
                    raise AssertionError("expected prototype capacity rejection")
                code, snapshot, _ = request(peer.port, "/config")
                assert code == 200 and snapshot["revision"] == successes
                _, state, _ = request(peer.port, "/status")
                original_log = state["log"]
                data_bytes = sum(len(base64.b64decode(e.get("data", ""))) for e in original_log)
                assert len(original_log) == successes+1
                if kind == "entry-count":
                    assert len(original_log) == 1024
                else:
                    assert data_bytes <= 4 << 20
                retry_code, retry_body, _ = request(peer.port, "/config", "PUT", update(0, fleet))
                assert retry_code == 503, (retry_code, retry_body)
                code, capture, _ = request(peer.port, "/checkpoint", "POST")
                assert code == 200, (code, capture)
                peer.stop()
                source_log, source_records = offline_log(peer.journal)
                assert source_log == original_log, "source lost acknowledged prefix"
                outcome = {"kind": kind, "workers_per_update": len(fleet), "acknowledged_updates": successes,
                           "log_entries": len(original_log), "command_payload_bytes": data_bytes,
                           "journal_bytes": peer.journal.stat().st_size, "journal_records": source_records,
                           "samples": samples, "writes": writes, "first_rejection": rejection,
                           "historical_retry_at_capacity": {"status": retry_code, "body": retry_body},
                           "checkpoint": capture, "restart": []}
                result["limits"].append(outcome)
                for mode in ("full", "checkpoint"):
                    restarted = Peer(root / f"{kind}-restart-{mode}")
                    shutil.copyfile(peer.journal, restarted.journal)
                    if mode == "checkpoint":
                        shutil.copyfile(peer.checkpoint, restarted.checkpoint)
                    restarted.start()
                    ready = restarted.ready(successes, allow_exit=True)
                    restarted.stop()
                    stored, _ = offline_log(restarted.journal)
                    assert stored[:len(original_log)] == original_log, "acknowledged log prefix lost"
                    if kind == "entry-count":
                        assert not ready["fresh_read"] and "1024 entries" in ready["log"], ready
                    else:
                        assert ready["fresh_read"] and stored[-1].get("noop"), ready
                    # Keep summaries, not multiple copies of large base64 log images.
                    state = ready.pop("state", None)
                    outcome["restart"].append({"mode": mode, **ready,
                        "retained_acknowledged_prefix": True,
                        "restored_checkpoint_index": state["checkpoint"]["recovery_index"] if state else None})

        result["recovery_summary"] = {mode: {"trials": trials,
            "listener_ready_ms_median": statistics.median(r["listener_ready_ms"] for r in result["recovery"] if r["mode"] == mode),
            "fresh_read_ready_ms_median": statistics.median(r["fresh_read_ready_ms"] for r in result["recovery"] if r["mode"] == mode)}
            for mode in ("full", "checkpoint")}
        result["passed"] = True
        print(f"PASS: {args.profile} controller capacity/recovery check; {output}")
    except Exception as error:
        result.update(passed=False, error=str(error))
        raise
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        for handle in handles:
            handle.close()
        result["process_logs"] = {str(path.relative_to(root)): path.read_text(encoding="utf-8") for path in root.glob("*/process.log")}
        result["files_retained"] = not result.get("passed", False)
        # root was resolved and checked within the explicit output directory above.
        if result.get("passed"):
            if root.resolve() != root or root.parent != output.parent:
                raise RuntimeError("experiment cleanup path changed")
            shutil.rmtree(root)
        output.write_text(json.dumps(result, indent=2)+"\n", encoding="utf-8")


if __name__ == "__main__":
    main()
