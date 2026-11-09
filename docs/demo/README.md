# Live terminal screen recording

![Actual live terminal demo](demo.gif)

[Full-resolution MP4](demo.mp4) ? [Raw console transcript](raw/terminal.txt) ? [Capture metadata](media.json)

This is a recording of an actual Windows console window while the demo executes.
FFmpeg captures the window through `gdigrab` at 10 frames per second. The MP4 is one
continuous real-time take: no generated terminal frames, overlays, page reconstruction,
or speed changes. The inline GIF uses the same captured pixels at 3 fps and the same
wall-clock pace. It loops automatically in the README.

Commands are scripted and executed during recording. The terminal displays real process
IDs, HTTP results, live generated tokens, failure actions, and recovery observations.
The console is attached to a real TTY; its window title/handle and capture command are
recorded in [console metadata](raw/console.json) and [session metadata](raw/session.json).
The selected take uses clean source `613ca9522d396d0daeafa7d532c9c98b875e08cd`.
The live demonstration took 84.031 seconds. The media file includes recorder startup/stop
frames. All MP4 frames were decoded successfully after capture; hashes are in metadata.

## What runs on screen

1. Two pinned llama.cpp CPU model servers and the Go gateway: real generation under
   round-robin, least-inflight, and prefix-affinity; two busy slots; HTTP 503 overload;
   client disconnects releasing both slots; live tokens from the next request.
2. Three native controller processes: an offline follower installs a retained snapshot
   and suffix, preserves original retry results, and survives leader loss/full restart.
3. Live generation through controller leader failure, drain/resume, snapshot catch-up,
   quorum loss, stale-view rejection, completion of the existing real stream, and a
   checkpoint-backed restart followed by new live tokens and a historical retry.

[Actual console text](raw/terminal.txt) and complete reports accompany the recording:
[worker pool](raw/real-pool.json), [three controllers](raw/replicated-retention.json),
[real-model recovery](raw/real-controller-failover.json). The
[FFmpeg capture log](raw/capture.log.txt) records input dimensions and encoding progress.
The earlier captured-output presentation has been replaced in the README and media files.

All processes run on one laptop. This does not establish GPU throughput, independent-host
resilience, or production readiness. Retention is manual; successful-request history is
bounded; deployment authentication and dynamic membership remain outside the local scope.
An earlier intermittent Windows connection reset remains documented in
[the historical failed test capture](failed-attempt-1/); no fix is claimed for it.

## Reproduce

Use Windows with the pinned local CPU model assets and `imageio-ffmpeg` available to Python.
From a clean committed checkout:

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
go build -o bin/gateway.exe ./cmd/gateway
python scripts/record_demo.py --output artifacts/demo/new-screen-run
```

The recorder opens a visible native console, waits for it to be ready, then starts window
capture before releasing the live demo. It records only that console window. Services
run during the capture and are cleaned up by their scenario runners. Failed assertions
prevent a successful session result. Source, console identity, raw reports, capture logs,
and the actual uncut MP4 remain in the output directory.
