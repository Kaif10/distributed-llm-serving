"""Download the pinned Windows x64 CPU engine and model; verify both SHA-256 hashes."""

import hashlib
import json
import os
from pathlib import Path
import platform
import urllib.request
import zipfile


ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def download(url, path, expected):
    if path.exists() and digest(path) == expected:
        print("Verified cached", path.name, flush=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    print("Downloading", path.name, flush=True)
    request = urllib.request.Request(url, headers={"User-Agent": "inference-fleet"})
    with urllib.request.urlopen(request, timeout=60) as response, partial.open("wb") as target:
        while chunk := response.read(1 << 20):
            target.write(chunk)
    if digest(partial) != expected:
        raise RuntimeError("SHA-256 mismatch: " + path.name)
    partial.replace(path)


def main():
    if os.name != "nt" or platform.machine().lower() not in ("amd64", "x86_64"):
        raise SystemExit("This pinned setup is for Windows x64. See the walkthrough for other systems.")
    config = json.loads((ROOT / "config/llama-cpu.json").read_text())
    engine = ROOT / ".tools" / ("llama-" + config["release"])
    archive = ROOT / ".tools" / ("llama-" + config["release"] + ".zip")
    download(config["archive_url"], archive, config["archive_sha256"])
    engine.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as bundle:
        for name in bundle.namelist():
            if not (engine / name).resolve().is_relative_to(engine.resolve()):
                raise RuntimeError("Archive entry outside engine directory")
        bundle.extractall(engine)
    model = ROOT / ".tools" / "models" / config["model_file"]
    url = "https://huggingface.co/" + config["model_repository"] + "/resolve/" + config["model_revision"] + "/" + config["model_file"]
    download(url, model, config["model_sha256"])
    print("Engine:", next(engine.rglob("llama-server.exe")))
    print("Model:", model)


if __name__ == "__main__":
    main()
