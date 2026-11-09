"""Run real scenarios, capture timestamped output/results, and export captioned MP4.

The video is an edited terminal/results playback, not a screen recording. Pauses
are shortened; raw wall-clock timestamps and complete reports remain alongside it.
Requires the pinned Windows model assets, Pillow, and imageio-ffmpeg (render only).
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import textwrap
import time

ROOT = Path(__file__).resolve().parents[1]


def render(output):
    from PIL import Image, ImageDraw, ImageFont
    import imageio_ffmpeg
    session = json.loads((output/"session.json").read_text(encoding="utf-8"))
    if not session.get("passed"):
        raise RuntimeError("refusing to export an incomplete/failed session as the working demo")
    events = [json.loads(line) for line in (output/"transcript.jsonl").read_text(encoding="utf-8").splitlines()]
    font_path = "C:/Windows/Fonts/consola.ttf" if platform.system() == "Windows" else "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
    font, title = ImageFont.truetype(font_path, 20), ImageFont.truetype(font_path, 30)
    lines = ["Inference Fleet | recorded working demo", "Source: "+session["source_commit"],
             "One laptop | real CPU LLMs | three controller processes",
             "Edited terminal/results playback; pauses shortened; no audio.",
             "Raw commands, timestamps, and complete JSON reports are included.", ""]
    for event in events:
        if event["kind"] in ("command", "stdout", "observed", "complete"):
            text = event["text"]
            if not isinstance(text, str):
                text = json.dumps(text, ensure_ascii=True)
            lines.extend(textwrap.wrap(f'[{event["elapsed_seconds"]:7.2f}s] {text}', width=100, subsequent_indent="    "))
            lines.append("")
    lines += ["", "Checks exercise mechanisms on one host, not GPU throughput or multi-host scale.",
              "Manual retention and finite request-history bounds remain.", "Full raw evidence: docs/demo/raw/"]
    pages = [lines[i:i+20] for i in range(0, len(lines), 20)]
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    command = [ffmpeg, "-y", "-f", "rawvideo", "-pixel_format", "rgb24", "-video_size", "1280x720",
               "-framerate", "1", "-i", "pipe:0", "-an", "-c:v", "libx264", "-crf", "23",
               "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(output/"demo.mp4")]
    with (output/"render.log").open("wb") as log:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=log, stderr=log)
        try:
            for number, page in enumerate(pages):
                frame = Image.new("RGB", (1280, 720), "#0b1220")
                draw = ImageDraw.Draw(frame)
                draw.rounded_rectangle((24, 22, 1256, 696), radius=18, fill="#121e30", outline="#33465d", width=2)
                draw.text((48, 40), "INFERENCE FLEET  /  LIVE RUN EVIDENCE", font=title, fill="#6de3c4")
                draw.text((48, 91), "clients -> gateway -> real llama.cpp workers  |  controller quorum: a / b / c", font=font, fill="#98b0cb")
                for row, line in enumerate(page):
                    draw.text((48, 136+row*25), line, font=font, fill="#e3edf8")
                draw.text((48, 658), f"Edited playback | raw timestamps shown | page {number+1}/{len(pages)} | single laptop", font=font, fill="#98b0cb")
                if number == 0:
                    frame.save(output/"poster.png")
                for _ in range(8):
                    process.stdin.write(frame.tobytes())
            process.stdin.close()
            if process.wait(timeout=90) != 0:
                raise RuntimeError("video export failed; inspect render.log")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    # Decode every frame to validate the exported media container and bitstream.
    subprocess.run([ffmpeg, "-v", "error", "-i", str(output/"demo.mp4"), "-f", "null", "-"], check=True, timeout=90)
    media = {"format": "captioned edited terminal/results playback; no audio; pauses shortened",
             "duration_seconds": len(pages)*8, "pages": len(pages), "resolution": [1280,720],
             "ffmpeg_version": imageio_ffmpeg.get_ffmpeg_version(), "command": command,
             "sha256": hashlib.sha256((output/"demo.mp4").read_bytes()).hexdigest(),
             "bytes": (output/"demo.mp4").stat().st_size, "full_decode_passed": True}
    (output/"media.json").write_text(json.dumps(media,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(media), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--render-only", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.render_only:
        render(output)
        return
    if (output/"session.json").exists():
        parser.error("choose a fresh capture directory; refusing to overwrite a session")
    source = subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()
    dirty = subprocess.check_output(["git","status","--porcelain"],cwd=ROOT,text=True)
    if dirty:
        parser.error("capture requires a clean committed source tree")
    started = time.monotonic()
    session = {"source_commit":source,"working_tree":"","platform":platform.platform(),
               "python":platform.python_version(),"started_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),
               "scope":"one laptop; real model generation and native controller processes; no scale claim",
               "format":"edited terminal/results playback; raw timestamped stdout preserved", "passed":False,
               "binary_sha256":{name:hashlib.sha256((ROOT/"bin"/name).read_bytes()).hexdigest() for name in ("gateway.exe","raftpeer.exe")}}
    with (output/"transcript.jsonl").open("w",encoding="utf-8") as transcript:
        def record(kind,text):
            entry={"elapsed_seconds":round(time.monotonic()-started,3),"kind":kind,"text":text}
            transcript.write(json.dumps(entry,ensure_ascii=True)+"\n");transcript.flush()
            print(f'[{entry["elapsed_seconds"]:7.2f}s] {text}',flush=True)
        def run(command):
            record("command",subprocess.list2cmdline(command))
            with subprocess.Popen(command,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding="utf-8",errors="replace") as process:
                try:
                    for line in process.stdout:
                        record("stdout",line.rstrip())
                    if process.wait()!=0:
                        raise RuntimeError("scenario failed: "+str(command))
                finally:
                    if process.poll() is None:
                        process.terminate();process.wait(timeout=15)
        try:
            run([sys.executable,"scripts/check_real_pool.py"])
            shutil.copyfile(ROOT/"artifacts/real-pool/results.json",output/"real-pool.json")
            pool=json.loads((output/"real-pool.json").read_text(encoding="utf-8"));assert pool["passed"]
            for trial in pool["runs"]:
                record("observed",{"policy":trial["policy"],"busy_real_engines":sum(s["is_processing"] for s in trial["busy_slots"]),
                                   "overload_status":trial["overload"]["status"],"busy_after_disconnect":sum(s["is_processing"] for s in trial["idle_after_disconnect"]),
                                   "completed_after_release":trial["after_release"]["done"],"generated_text":trial["after_release"]["text"]})
            run([sys.executable,"scripts/check_replicated_retention.py","bin/raftpeer.exe","--output",str(output/"replicated-retention.json")])
            peers=json.loads((output/"replicated-retention.json").read_text(encoding="utf-8"));assert peers["passed"]
            record("observed",{"installed_boundary":peers["installed_peer"]["retention"]["installed_index"],
                               "caught_up_revision":peers["installed_peer"]["configuration"]["revision"],
                               "original_retry_result":peers["original_retry_result"],
                               "revisions_after_full_restart":{n:s["configuration"]["revision"] for n,s in peers["after_full_restart"].items()}})
            run([sys.executable,"scripts/check_controller_failover.py","--retention","--output",str(output/"real-controller-failover.json")])
            live=json.loads((output/"real-controller-failover.json").read_text(encoding="utf-8"));assert live["passed"]
            record("observed",{"leader_failure":live["observations"],"active_real_stream_survived_expiry":live["active_stream_survived_expiry"],
                               "restored_boundary":live["retained_restarted_peer"]["retention"]["recovered_index"],
                               "historical_retry_revision":live["historical_retry_after_recovery"]["revision"],
                               "new_generation_after_recovery":live["generation_after_recovery"]["done"],
                               "generated_text":live["generation_after_recovery"]["text"]})
            session["passed"]=True
            record("complete","PASS: real model routing/admission/cancellation; peer snapshots and full restart; gateway quorum expiry and recovery.")
        finally:
            session["wall_seconds"]=round(time.monotonic()-started,3)
            (output/"session.json").write_text(json.dumps(session,indent=2)+"\n",encoding="utf-8")
    render(output)


if __name__=="__main__":
    main()
