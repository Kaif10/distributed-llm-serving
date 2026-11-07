# Increment 19: keep gateway configuration fresh across controller leadership changes

The replicated configuration API can elect a new leader, but a gateway previously polled
one fixed controller URL. A dead or isolated old leader therefore caused its view to expire
even when another majority could serve fresh configuration. The gateway now accepts a
bounded peer list and tries another configured endpoint when a read fails.

```powershell
go build -o bin/gateway.exe ./cmd/gateway
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/check_controller_failover.py --output artifacts/controller-failover/run.json
```

The experiment uses the already installed, pinned Windows CPU llama.cpp and SmolLM2-135M
assets. It downloads nothing and verifies the model checksum before launch. It starts one
real inference engine, three configuration peers with separate journals, and one gateway.
The output records commands, binary hashes, model pins, source/working-tree state, actual
generation results, selected controller origins, revision/expiry observations, and failures.

## Configure the gateway

For the three-peer setup from the earlier walkthroughs:

```powershell
.\bin\gateway.exe -listen 127.0.0.1:18080 -admin-listen 127.0.0.1:18090 `
  -backend http://127.0.0.1:18081 `
  -controllers http://127.0.0.1:19301,http://127.0.0.1:19302,http://127.0.0.1:19303 `
  -config-poll 100ms -config-timeout 900ms -config-max-age 3s
```

Use either `-controller` (the existing single endpoint) or `-controllers` (1-9 origins).
Both forms preserve the existing fixed worker-pool validation. Unknown membership/origin
changes, rollback, and conflicting same-revision snapshots do not refresh the view.
The peer list is gateway startup configuration, not a Raft membership change.

## Selection, deadlines, and freshness

The gateway first tries the endpoint that last supplied an accepted snapshot. If that
request fails, returns non-200, or supplies an invalid snapshot, it tries the other configured
origins in order. It does not follow redirects or trust leader hints. If all peers fail,
it rotates the starting peer for the next cycle. Successful fallback clears the cycle error.

`-config-timeout` is one network budget for the entire poll cycle. Each attempt receives
the remaining time divided by the remaining candidates. Fast failures leave additional
time for later peers. This prevents a reachable isolated old leader's five-second server
read deadline from consuming the entire client cycle before another peer is tried.
Timeouts that are too short for a quorum round trip can still cause view expiration.

Only a successfully decoded, validated, applied snapshot renews freshness. Expiry is measured
from that attempt's start, preserving a conservative maximum view age that includes fetch
latency. Merely contacting a peer, receiving 503, or retaining an old cached snapshot cannot
extend the deadline. A same-revision snapshot with identical workers can reconfirm the view.

The admin API's GET `/config` includes `controller_origin`, identifying the last successful
source alongside applied revision, freshness, expiry, and the last failed-cycle error.
This source remains visible when the view expires; it is not proof of current leadership.
Generation still uses a local snapshot. It performs no controller calls on the token path.

All supplied endpoints must belong to the **same replicated configuration group** and expose
its fresh GET `/config` contract. The gateway does not authenticate cluster identity or make
an arbitrary cached HTTP 200 response fresh. Mixing independent single-writer services is
unsupported. Administrative writes still target a leader directly; this poller retries reads
only and does not retry model generation.

## Follow the real-model failure trace

1. Commit revision 1, start the gateway with all three client origins, and complete a real
   streamed generation through the gateway.
2. Kill its selected controller leader. The two survivors elect a replacement. Observe the
   gateway switch `controller_origin` while keeping revision 1 confirmed.
3. Commit a drain through the new leader. Observe revision 2 applied and new inference
   rejected. Commit a resume and complete another generation at revision 3.
4. Start a long real generation, then kill another controller so only one remains. Its
   local leadership status cannot substitute for quorum confirmation. The gateway view
   expires, readiness fails, and the already admitted stream continues to `[DONE]`.
5. With the engine idle and the view still expired, a new inference request must get 503.
   Restart one peer from its journal, restore quorum, reconfirm revision 3, and complete
   a new real generation.

The raw results distinguish controller commitment, gateway application, and stream completion.
A desired drain acknowledgement does not itself establish that all gateways have applied it
or finished active work. This experiment has one gateway and one inference worker on one
host. A single observed failover duration is not an availability or latency guarantee.

## Verification and remaining limits

Go HTTP tests exercise a hung first endpoint, follower responses, successful-peer preference,
leader/source changes, rollback rejection without freshness renewal, expiry, recovery,
input bounds, and redirect refusal. Existing stream/expiry tests continue to cover active
request preservation. Linux CI runs the Go race tests and existing protocol process checks;
the pinned Windows real-model experiment is a separate recorded local run.

The serving pool remains fixed. There is no dynamic controller discovery, remote security,
multi-host deployment, or protection from loss of the shared host/disk. Replicated writes,
retries, conflicts, and leader no-ops still consume a bounded log; reads no longer do.

Next: design complete application checkpoints, including successful request-ID results,
before implementing log retention/compaction. The public configuration snapshot alone
does not contain enough information for retry-safe recovery.
