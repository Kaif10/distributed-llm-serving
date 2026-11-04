"""Check the foundation through a real process and TCP connection (Python 3.10+)."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def check(binary: Path) -> None:
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as log:
        process = subprocess.Popen(
            [str(binary), "-listen", "127.0.0.1:0"],
            stdout=log, stderr=log, creationflags=flags,
        )
        try:
            deadline = time.monotonic() + 10
            address = None
            while time.monotonic() < deadline:
                log.seek(0)
                output = log.read()
                match = re.search(r"address=(127\.0\.0\.1:\d+)", output)
                if match:
                    address = match.group(1)
                    break
                if process.poll() is not None:
                    raise RuntimeError("Startup failed: " + output)
                time.sleep(0.05)
            assert address, "No listener address within startup deadline"
            url = "http://" + address
            with urllib.request.urlopen(url + "/healthz", timeout=3) as response:
                assert response.status == 200
                assert response.headers["Content-Type"] == "application/json"
                assert json.load(response) == {"status": "ok"}
            for path, method, expected in [
                ("/v1/chat/completions", "POST", 404),
                ("/healthz", "POST", 405),
            ]:
                request = urllib.request.Request(url + path, method=method)
                try:
                    with urllib.request.urlopen(request, timeout=3):
                        raise AssertionError("Expected HTTP rejection")
                except urllib.error.HTTPError as err:
                    assert err.code == expected, (err.code, expected)
                    err.close()
            other = subprocess.run(
                [str(binary), "-listen", address], capture_output=True,
                text=True, timeout=5, creationflags=flags,
            )
            assert other.returncode != 0, "Occupied port must fail startup"
            assert "gateway listening" not in other.stderr
            print("PASS: health JSON, generation 404, method 405, occupied-port failure")
            if os.name != "nt":
                process.terminate()  # SIGTERM invokes the service's graceful shutdown.
                assert process.wait(timeout=7) == 0, "Graceful shutdown must succeed"
                log.seek(0)
                assert "gateway draining" in log.read()
                print("PASS: SIGTERM graceful shutdown with no active requests")
            else:
                print("SKIP: SIGTERM shutdown check on Windows")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    check(args.binary.resolve(strict=True))
