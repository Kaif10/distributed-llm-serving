"""Record a real Windows console window with FFmpeg gdigrab while live services run."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import imageio_ffmpeg

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("this recorder captures an actual Windows console window")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output/"console.json").exists():
        parser.error("choose a fresh capture directory")
    dirty = subprocess.check_output(["git","status","--porcelain"],cwd=ROOT,text=True)
    if dirty:
        parser.error("commit the source before recording")
    source = subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    driver_command = ["C:/Windows/System32/conhost.exe",sys.executable,"-u",str(ROOT/"scripts/live_demo.py"),"--output",str(output)]
    driver = subprocess.Popen(driver_command,cwd=ROOT,creationflags=subprocess.CREATE_NEW_CONSOLE)
    recorder = None
    session = {"source_commit":source,"working_tree":"","platform":platform.platform(),
               "started_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),
               "format":"actual Windows console window captured by FFmpeg gdigrab; continuous real-time recording",
               "driver_command":driver_command,"passed":False}
    try:
        deadline=time.monotonic()+20
        while not (output/"console.json").exists():
            if driver.poll() is not None or time.monotonic()>deadline:
                raise RuntimeError("real console did not become ready")
            time.sleep(.1)
        console=json.loads((output/"console.json").read_text())
        if not console["stdout_is_console"]:
            raise RuntimeError("driver is not attached to a real console")
        command=[ffmpeg,"-y","-f","gdigrab","-framerate","10","-draw_mouse","0","-i","title="+console["title"],
                 "-vf","pad=ceil(iw/2)*2:ceil(ih/2)*2","-an","-c:v","libx264","-preset","veryfast","-crf","18",
                 "-pix_fmt","yuv420p","-movflags","+faststart",str(output/"demo.mp4")]
        session["capture_command"]=command
        with (output/"capture.log").open("wb") as log:
            recorder=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=log,stderr=log)
            deadline=time.monotonic()+15
            while "Output #0" not in (output/"capture.log").read_text(errors="replace"):
                if recorder.poll() is not None or time.monotonic()>deadline:
                    raise RuntimeError("gdigrab capture did not start")
                time.sleep(.1)
            (output/"start.flag").write_text("capture running")
            print("RECORDING actual console:",console["title"],flush=True)
            deadline=time.monotonic()+300
            while not (output/"finished.json").exists():
                if driver.poll() is not None or recorder.poll() is not None or time.monotonic()>deadline:
                    raise RuntimeError("capture or live demo exited unexpectedly")
                time.sleep(.2)
            recorder.stdin.write(b"q\n");recorder.stdin.flush()
            if recorder.wait(timeout=30)!=0:
                raise RuntimeError("capture export failed")
        finished=json.loads((output/"finished.json").read_text())
        if not finished["passed"]:
            raise RuntimeError("live demo failed; preserving actual capture")
        driver.wait(timeout=15)
        subprocess.run([ffmpeg,"-v","error","-i",str(output/"demo.mp4"),"-f","null","-"],check=True,timeout=120)
        gif=[ffmpeg,"-y","-v","error","-i",str(output/"demo.mp4"),"-filter_complex",
             "fps=5,scale=1000:-1:flags=lanczos,split[a][b];[a]palettegen[p];[b][p]paletteuse=dither=bayer","-loop","0",str(output/"demo.gif")]
        subprocess.run(gif,check=True,timeout=120)
        subprocess.run([ffmpeg,"-y","-v","error","-ss","3","-i",str(output/"demo.mp4"),"-frames:v","1",str(output/"poster.png")],check=True,timeout=30)
        session.update(passed=True,wall_seconds=finished["wall_seconds"],console=console,
                       uncut=True,speed=1.0,overlays=False,generated_frames=False,
                       ffmpeg_version=imageio_ffmpeg.get_ffmpeg_version(),full_decode_passed=True,
                       media={name:{"bytes":(output/name).stat().st_size,"sha256":hashlib.sha256((output/name).read_bytes()).hexdigest()} for name in ["demo.mp4","demo.gif"]})
        print(json.dumps(session),flush=True)
    finally:
        if recorder is not None and recorder.poll() is None:
            recorder.kill();recorder.wait()
        if driver.poll() is None:
            driver.terminate();driver.wait()
        (output/"session.json").write_text(json.dumps(session,indent=2)+"\n",encoding="utf-8")

if __name__=="__main__":
    main()
