# Increment 26: restore and retain the single-controller application

The previous increment added coherent two-slot storage without connecting it to a
service. This increment connects it to an explicit single-member configuration mode.
The serialized controller owner creates application images and advances the local
boundary only after the committed prefix has been applied and its pending results
have been handled. Three-peer prefix removal remains disabled until lagging peers can
install snapshots.

## Run the alternative storage mode

Use a fresh directory; this does not convert existing journals:

```powershell
go build -o bin/raftpeer.exe ./cmd/raftpeer
bin/raftpeer.exe -id a -peers a=http://127.0.0.1:19201 -client-listen 127.0.0.1:19301 -configuration -retained-dir artifacts/local-controller/store -retained-group local-controller
```

This uses the existing `GET /config`, `PUT /config`, and `GET /status` APIs. On the client
listener, `POST /retain` with an empty body retains the fully applied committed prefix.
It returns its absolute index, term, checkpoint digest/size, and recovered boundary.
Status also reports this metadata and the retained suffix. It never returns application
image bytes through the diagnostic API.

Do not combine `-retained-dir` with `-journal` or `-checkpoint-journal`. Raw proposal mode,
multi-member specifications, and a missing/mismatched group identity are rejected.
The original `-journal` mode and three-peer checks are unchanged. The new maintenance
route is absent there. All listeners remain trusted loopback listeners without TLS or
authentication. A single member provides persistence but no controller redundancy.

Retention is manual preventive maintenance in this increment. Invoke it while a leader
can still serve, before exhausting the suffix. There is no automatic threshold or repair
tool for an already full unretained restart. The alternative storage still bounds its
suffix at 1024 entries/four MiB and its two file payloads at 24 MiB each.

## One request and restart trace

1. Elect the single controller, committing no-op index 1.
2. Commit `drain` at index 2 (revision 1), then `resume` at index 3 (revision 2).
3. The owner handles `POST /retain`: its machine encodes configuration plus every original
   successful request/result, bound to applied/committed index 3 and its log term.
4. `Node.RetainLocal` verifies delivery, index, term, storage type, leadership, and
   single-member membership. It publishes the durable image before changing core state.
   The in-memory suffix becomes empty while commit/applied stay at absolute index 3.
   The older file generation remains until overwritten by a subsequent publication.
5. A retry of `drain` is logged at index 4 and returns the original revision-1 draining
   result. It does not change current revision 2.
6. Kill the process. Reopen selects checkpoint 3 with suffix entry 4. The constructor
   decodes and validates the real application image and restores it at boundary 3.
7. A new leader no-op at index 5 confirms the suffix. The owner applies entry 4 as a retry,
   preserving current revision 2. Only then can the fresh-read API return successfully.

Unit tests also retain an earlier prefix, append a genuine new update, reopen, and compare
checkpoint-plus-suffix application state against its fully applied pre-restart state.
They verify the original request result separately; equal current configuration alone
would not establish preservation of retry history.

## Safety and failure behavior

Only `NewConfigurationWithRetention` can enable the restored path. Default configuration
construction and the raw runtime still reject a nonzero boundary. Retained storage is
bound to the local member and configured group; the node rejects another member's store.
Invalid application bytes or a position differing from the durable boundary fail startup.

Local retention requires a single-member leader and a fully delivered committed prefix.
The host adds the application-applied check and constructs the image from its own machine;
clients cannot submit arbitrary image bytes or claim a commit index. Repeating maintenance
at the same boundary returns the same metadata without another publication. Existing
term-scoped reads remain valid across successful local retention.

Uncertain publication I/O fences the store and core and stops the owner. Reopen must
select/sync complete files before resuming. A disconnected/timed-out maintenance client
may have an unknown outcome; inspect status or retry the same-prefix operation. Ordinary
configuration retries still return their original successful results.

The existing application codec has a 1024-successful-request/eight-MiB payload limit.
A retention request that exceeds codec bounds returns 503 without evicting IDs or
stopping reads. No automatic admission policy or unbounded successful-ID retention is
introduced. Retention does not turn the controller into an unlimited-lifetime service.

## Reproduce native-process evidence

```powershell
go test ./...
go vet ./...
go build -o bin/raftpeer.exe ./cmd/raftpeer
python scripts/check_retained_controller.py bin/raftpeer.exe --output artifacts/retained-controller/full.json
```

Pass `--go <executable>` if Go is not on PATH. `--retries 64` is the shorter CI check.
The default sends two unique updates followed by 1100 logged historical retries, retaining
every 64 retry commands. The first kill occurs at commit index 1103, after an unretained
suffix; the second follows another retention. Both restarts must confirm current revision
2 and preserve the original revision-1 result. The runner checks same-prefix maintenance,
body rejection, suffix/application indices, recovered image hashes, and bounded file
lengths. Raw JSON includes source/binary identity, commands, captures, and restart status.
It kills only its own processes, removes its own successful temporary directory, and
preserves failed run files.

This is a sequential administrative correctness check on one machine, not inference,
throughput, an availability test with redundant controllers, or 1100 unique successes.
The previous process-kill tests cover interrupted storage publication; this runner kills
the actual service after acknowledged operations. Power/device failures, physical disk
allocation, and multi-host behavior remain unvalidated.

Next implement snapshot installation and recovery for a lagging peer before enabling
retention in the three-member group. Rerun real-model gateway failure checks against that
integrated path; the final working-demo recording remains outstanding.

Exercise: after restoring the committed image, why does the controller still require a
new-term commit before a fresh read? Why can't a three-member leader simply delete its
old prefix while a disconnected follower still needs those entries?
