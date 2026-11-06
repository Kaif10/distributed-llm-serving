# Increment 12: distinguish saved, applied, and drained

The gateway can now consume desired drain settings from the durable controller. This
increment keeps worker membership fixed: configure the same origins in the gateway and
controller. New/removed origins are rejected by the consumer until a later lifecycle design.

## Run it

Start the real engine using scripts/run_llama.ps1 on port 18081. In other terminals, from
the repository root, using a fresh journal path for this example:

```powershell
go run ./cmd/controller -journal artifacts/follow-demo/fleet.wal
curl.exe -X PUT -H "Content-Type: application/json" --data-binary "@examples/config-enable.json" http://127.0.0.1:19090/config
go run ./cmd/gateway -backend http://127.0.0.1:18081 -controller http://127.0.0.1:19090 -admin-listen 127.0.0.1:18090
```

Use the local Go path from earlier walkthroughs if needed. Existing journals require their
current expected revision for new updates. An old request ID retry returns its original
result; it does not necessarily describe current state.

```powershell
curl.exe http://127.0.0.1:18090/config
curl.exe http://127.0.0.1:18090/workers
curl.exe --no-buffer -H "Content-Type: application/json" --data-binary "@examples/real-stream.json" http://127.0.0.1:8080/v1/chat/completions
```

The admin status shows managed/applied flags, applied revision, confirmation/expiry times,
current freshness, and the latest fetch error. Worker observations include the controller's
logical config_id as well as the local numeric index. Existing worker counters and streams
remain attached to their original proxy; snapshot application updates desired drain flags
under the same mutex used for reservation.

## Acknowledgement is not application

1. Controller PUT acknowledges a journal-synced revision.
2. Gateway fetches and validates the snapshot, then atomically applies drain flags and revision.
3. Existing admitted requests finish. A draining worker becomes locally drained at zero inflight.

These are separate observations. Controller acknowledgement does not wait for any gateway.
In controller-managed mode, local gateway drain/resume POSTs return 409 to prevent competing
sources of desired state. Update through controller CAS, then inspect applied revision and
worker state. There is no fleet-wide drain-completion guarantee yet.

## Freshness and failure

Before the first acceptable snapshot, readiness and admission fail with 503. Fetches default
to every 500 ms with a one-second timeout and maximum view age five seconds. Configure using
`-config-poll`, `-config-timeout`, and `-config-max-age` (age must exceed fetch timeout).
Health probing stays independent of controller availability.

The gateway uses local monotonic elapsed time and dates expiry from the start of a successful
fetch. It does not trust a server clock. Same-revision identical snapshots can confirm the
view again. Older revisions, conflicting same-revision contents, invalid JSON, fetch errors,
and mismatched membership do not refresh it or partially apply state. Snapshots are bounded
to one MiB. Controller responses forbid storage by HTTP caches and fetches request revalidation.

During an outage, a previously confirmed view can serve new work until expiry. Once expired,
new admissions and readiness fail; liveness still responds and existing streams may finish.
Reservations acquired before expiry or drain can still be reading their request bodies;
they remain admitted work and may proceed. Expiry does not revoke earlier reservations.
Restarting the durable controller can confirm the same revision and restore freshness.
If its revision regresses, the gateway rejects that view and remains subject to expiry.

This deliberately trades availability for bounded use of an unconfirmed view. A drain can
commit while a disconnected gateway still admits work from its unexpired old view. The age
bound does not make the old configuration current. Gateway restart has no local persistent
view and requires a new confirmation. Static mode without -controller retains local admin behavior.

## Verify with a real model

```powershell
go build -o bin/controller.exe ./cmd/controller
go build -o bin/gateway.exe ./cmd/gateway
python scripts/check_config_follow.py
```

The script owns an engine, controller, gateway, and temporary journal. It verifies applied
drain during a real 256-token stream, expiry during a real 1024-token stream, rejection with
an expired view after capacity becomes idle, and same-revision recovery after controller
restart. It cleans up its processes. Raw results and logs go to artifacts/config-follow/.
Controlled network tests additionally exercise rollback/conflict rejection, local-admin
exclusion, unsupported membership, and recovery. These are not Raft or partition proofs.

Next: define and test the deterministic controller state machine before replicating its
commands with Raft. Dynamic worker membership and fleet-wide application acknowledgements
remain explicit follow-up work.
