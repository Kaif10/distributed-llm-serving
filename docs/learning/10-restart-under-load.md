# Increment 10: recover capacity while arrivals continue

The earlier failure test sent requests around a restart in separate steps. This experiment
keeps fixed-arrival traffic running throughout process loss, the outage, and recovery.
Every generated response comes from the pinned real CPU model.

```powershell
go build -o bin/gateway.exe ./cmd/gateway
python scripts/check_restart_load.py --output-dir artifacts/restart-load-aligned
```

Use the local Go executable path if needed. The model setup is unchanged. The script owns
two llama.cpp processes, one gateway, and a loopback admin listener. Ports 18080, 18081,
18082, and 18090 must be free. It cleans up all children after each run.

## Traffic and failure schedule

Each run first starts a 1024-token control generation and consumes its real stream in a
background thread. That ensures an engine is actively working when the experiment kills it.
This intentional failure is recorded separately from the offered traffic cohort.

A separate generator offers 32 requests at two per second, each requesting 16 output tokens.
The control and traffic requests use the same prefix label. Both policies see the same
request payloads and arrival schedule. Client threads continue independently of responses.

Two seconds after arrivals begin, the harness confirms the control worker is still busy
and terminates its process. It observes the gateway marking that worker unhealthy, verifies
that the control stream failed without `[DONE]`, waits two seconds, and launches a new
engine process at the same origin. Health probes must restore eligibility. Traffic continues
while the model reloads, and subsequent requests must complete on the restarted worker.

Both policies run for three trials, with fresh processes each time. Policy order is fixed
least-inflight then affinity, so this correctness experiment does not support a timing or
performance ranking between them. Probe interval is 100 ms and timeout 500 ms; those differ
from default production-process settings. Workers each have two CPU threads and one slot.

## Assertions

- The killed engine was actively processing the separate control generation.
- The control stream reports an error or EOF without a completion marker; it is not resumed.
- A request scheduled during the observed outage completes on the survivor.
- A request scheduled after observed recovery completes on the restarted worker.
- Every offered request has a completed, rejected, or failed outcome, with no hidden retries.
- At the end, both gateway admitted-request counts are zero and both engine slots are idle.

Raw samples, original/restarted process IDs, engine state, worker identity, and observed
event times go under the selected output directory. Per-run diagnostics are written on
failure too. Observed timestamps describe polling, not exact internal transition times.
The background 32-request cohort excludes the one intentionally interrupted control stream;
read both records when assessing failures.

The header and label identify local worker placement, not persistent process identity.
Restarting the same origin loses its engine cache, while its gateway index remains the
same. Later control-plane work will need an explicit incarnation/lifecycle contract.

## What this does not promise

The recorded six runs completed all 192 traffic requests. Six separate, intentionally
interrupted control generations failed explicitly. Every run observed survivor progress,
new work on the restarted process, and final zero reservation counts/idle slots. See the
[evidence ledger](../EVIDENCE.md) for raw observations and limits on this conclusion.

A failure can race a successful health probe. Requests may still receive 502 or broken
streams during detection. Recovery requires a healthy model server; a new process existing
is insufficient. The assertions require survivor progress, not lossless or exactly-once
inference. There is no multi-host partition, disk loss, gateway restart, or model warm-cache
guarantee. Successful runs do not resolve the previous unexplained generation timeout.

The first harness attempt used different affinity labels for the control and traffic and
incorrectly required traffic to return to a worker it did not prefer. That assertion failed
legitimately. The corrected test shares the label, and the initial result is retained in
the evidence ledger. No gateway change was needed for that correction.

Next: persist versioned fleet configuration so administrative changes have an explicit
acknowledgement and restart contract. Existing-router performance comparison remains a
separate outstanding evaluation; these tests do not establish superiority over one.
