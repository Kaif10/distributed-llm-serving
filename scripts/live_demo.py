"""Scripted live demonstration inside a real Windows console (not playback)."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def console(title):
    k, u = ctypes.windll.kernel32, ctypes.windll.user32
    k.GetConsoleWindow.restype = ctypes.c_void_p
    k.GetStdHandle.restype = ctypes.c_void_p
    k.SetConsoleTitleW(title)
    hwnd = k.GetConsoleWindow()
    class Coord(ctypes.Structure):
        _fields_ = [("X", ctypes.c_short), ("Y", ctypes.c_short)]
    class Rect(ctypes.Structure):
        _fields_ = [("Left", ctypes.c_short), ("Top", ctypes.c_short), ("Right", ctypes.c_short), ("Bottom", ctypes.c_short)]
    class Font(ctypes.Structure):
        _fields_ = [("cbSize", ctypes.c_ulong), ("nFont", ctypes.c_ulong), ("dwFontSize", Coord),
                    ("FontFamily", ctypes.c_uint), ("FontWeight", ctypes.c_uint), ("FaceName", ctypes.c_wchar*32)]
    class Info(ctypes.Structure):
        _fields_ = [("size", Coord), ("cursor", Coord), ("attributes", ctypes.c_ushort),
                    ("window", Rect), ("maximum", Coord)]
    handle = k.GetStdHandle(-11)
    k.SetCurrentConsoleFontEx.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.POINTER(Font)]
    font = Font(ctypes.sizeof(Font), 0, Coord(0, 16), 54, 400, "Consolas")
    k.SetCurrentConsoleFontEx(handle, False, ctypes.byref(font))
    k.SetConsoleWindowInfo.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.POINTER(Rect)]
    k.SetConsoleScreenBufferSize.argtypes = [ctypes.c_void_p, Coord]
    k.SetConsoleWindowInfo(handle, True, ctypes.byref(Rect(0, 0, 79, 19)))
    k.SetConsoleScreenBufferSize(handle, Coord(100, 3000))
    k.SetConsoleWindowInfo(handle, True, ctypes.byref(Rect(0, 0, 99, 27)))
    k.GetConsoleScreenBufferInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(Info)]
    info = Info()
    if not k.GetConsoleScreenBufferInfo(handle, ctypes.byref(info)):
        raise RuntimeError("cannot inspect actual console viewport")
    # Match the buffer to the actual DPI-constrained viewport so long commands
    # wrap visibly rather than disappearing into a horizontal scroll area.
    columns = info.window.Right-info.window.Left+1
    if not k.SetConsoleScreenBufferSize(handle, Coord(columns, 3000)):
        raise RuntimeError("cannot fit console buffer to visible window")
    u.ShowWindow.argtypes = [ctypes.c_void_p, ctypes.c_int]
    u.SetForegroundWindow.argtypes = [ctypes.c_void_p]
    u.SetWindowPos.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_uint]
    u.ShowWindow(hwnd, 1)
    u.SetWindowPos(hwnd, None, 30, 30, 0, 0, 0x0001 | 0x0004)
    u.SetForegroundWindow(hwnd)
    return hwnd


class Tee:
    def __init__(self, terminal, file):
        self.terminal, self.file = terminal, file
    def write(self, value):
        self.terminal.write(value)
        self.file.write(value)
        self.flush()
        return len(value)
    def flush(self):
        self.terminal.flush()
        self.file.flush()
    def isatty(self):
        return self.terminal.isatty()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    title = "Inference Fleet - REAL LIVE DEMO " + str(os.getpid())
    hwnd = console(title)
    with (output/"terminal.txt").open("w", encoding="utf-8") as transcript:
        sys.stdout = Tee(sys.stdout, transcript)
        sys.stderr = sys.stdout
        print("INFERENCE FLEET - REAL LIVE TERMINAL DEMO")
        print("Scripted commands execute real services. This is not result playback.")
        print("One laptop | real llama.cpp CPU generation | three Raft controllers")
        print("Source:", subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip())
        print()
        (output/"console.json").write_text(json.dumps({"title": title, "hwnd": hwnd, "pid": os.getpid(), "stdout_is_console": sys.stdout.isatty()}), encoding="utf-8")
        deadline = time.monotonic()+30
        while not (output/"start.flag").exists():
            if time.monotonic() > deadline:
                raise RuntimeError("screen recorder did not signal start")
            time.sleep(.05)
        started = time.monotonic()
        def run(label, command):
            print("\n"+"="*90)
            print(label)
            print("EXECUTE:", subprocess.list2cmdline(command))
            print("="*90, flush=True)
            time.sleep(2)
            with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", creationflags=subprocess.CREATE_NO_WINDOW) as process:
                for char in iter(lambda: process.stdout.read(1), ""):
                    sys.stdout.write(char)
                if process.wait() != 0:
                    raise RuntimeError("live scenario failed")
            print("\nCommand finished successfully. Elapsed real time:", round(time.monotonic()-started, 1), "seconds", flush=True)
            time.sleep(3)
        passed = False
        try:
            time.sleep(3)
            run("1. REAL MODEL ROUTING, OVERLOAD, CANCELLATION, LIVE TOKENS", [sys.executable, "-u", "scripts/check_real_pool.py", "--live"])
            import shutil
            shutil.copyfile(ROOT/"artifacts/real-pool/results.json", output/"real-pool.json")
            run("2. THREE CONTROLLERS: OFFLINE FOLLOWER, SNAPSHOT, LEADER LOSS, FULL RESTART", [sys.executable, "-u", "scripts/check_replicated_retention.py", "bin/raftpeer.exe", "--output", str(output/"replicated-retention.json")])
            run("3. LIVE LLM THROUGH CONTROLLER FAILURE, QUORUM LOSS, AND DURABLE RECOVERY", [sys.executable, "-u", "scripts/check_controller_failover.py", "--retention", "--live", "--output", str(output/"real-controller-failover.json")])
            print("\nLIVE DEMO COMPLETE - all scenario assertions passed.")
            print("Actual window capture. No synthetic terminal frames or reconstructed results.")
            print("Local validation only; manual retention and finite history limits remain.")
            passed = True
            time.sleep(6)
        finally:
            (output/"finished.json").write_text(json.dumps({"passed": passed, "wall_seconds": round(time.monotonic()-started, 3)}), encoding="utf-8")
            sys.stdout.flush()
            sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__


if __name__ == "__main__":
    main()
