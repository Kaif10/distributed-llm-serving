# Increment 20: preserve configuration and retry history in an application checkpoint

The serving Raft log is bounded. Before removing any prefix, recovery needs more than the
public configuration view: it also needs every retained successful request's original
update and result. Otherwise retrying an acknowledged old operation after compaction could
return a conflict or apply it again. This increment defines and validates that application
checkpoint, with an offline file-backed recovery example. It does not enable live snapshots
or truncate serving journals.

```powershell
go test ./internal/control -run Checkpoint -v
go run ./cmd/checkpointdemo -output artifacts/checkpoint/result.json
```

The demo runs one actual Raft core with explicit ticks and a real local journal. It writes,
syncs, closes, and reopens a checkpoint file, then compares checkpoint-plus-suffix recovery
with full application replay. It is not a network deployment, serving-controller restart,
or inference experiment. Generated files stay under the ignored artifacts directory.

## Checkpoint contents and framing

`Machine.Checkpoint(position)` encodes:

- Format kind `fleet-application-v1`.
- The owner-supplied applied log index and the term of the entry at that index.
- Current configuration revision and workers.
- Every retained successful request's original update and result revision, ordered by
  revision. The original workers plus result revision reconstruct that request's result.

These are the current machine's complete recovery inputs. Failed CAS commands and retries
do not add successful-request records or change the current revision. Historical successful
IDs are not evicted. A checkpoint still grows with distinct successful requests; this format
is not a solution for unbounded deduplication history.

One frame contains a little-endian length, payload CRC32, header CRC32, and canonical JSON.
The decoder requires a complete frame, exact length, supported kind, valid checksums, and
no trailing bytes. It rejects unknown fields and noncanonical forms, including duplicate
keys, rather than accepting multiple encodings of one checkpoint. Limits are eight MiB of
encoded payload and 1024 successful requests, matching the bounded prototype's current
maximum entry count. Export also checks a raw-string budget before JSON allocation.
The codec returns an error at its bound; it never silently drops request IDs or changes
the live machine. This does not change the single-writer Store's existing Apply behavior.

`RestoreCheckpoint` builds a fresh machine. It validates successful revision order, expected
revisions, uniqueness of IDs, valid worker records, and that replay reconstructs exactly the
recorded current state. It returns no machine on failure. Caller-owned byte buffers and
returned worker slices cannot mutate the restored retry history or the original machine.

## Position and trust boundary

The serialized owner must capture checkpoint state and its applied position together,
after applying a committed prefix. The pure codec can validate zero/nonzero position pairs
and that configuration revision does not exceed log index. It cannot establish consensus,
authenticate a group, or prove that an arbitrary supplied position describes that machine.
Checksums detect accidental corruption, not malicious replacement.

An installer therefore needs a trusted capture/publication contract and group/log binding.
It must not publish application state ahead of established commitment. The offline demo
checks the position against the committed journal entry, rejects a mismatched boundary term,
and also compares full checkpoint images against complete replay. A matching index/term
alone does not authenticate checkpoint contents or group identity.

The checkpoint boundary term is **not the current election term**. Application restoration
must never roll back the Raft term/vote. After reopening the demo's journal, volatile commit
knowledge is zero; its term-2 leader no-op re-establishes commitment before suffix replay.

## Follow the recovery trace

1. Elect a single-member leader: no-op at index 1, term 1.
2. Commit a drain and resume at indices 2/3, reaching configuration revision 2.
3. Capture checkpoint position `(3, 1)`. It retains the original drain result at revision 1
   as well as the resumed current configuration at revision 2.
4. Append a successful retry, a stale CAS conflict, and another update. Only the new update
   advances current configuration to revision 3.
5. Close/reopen the Raft journal and read the checkpoint file. Refuse to treat its position
   as confirmed while volatile commit index is zero.
6. Elect the term-2 leader and commit its no-op at index 7. Validate the checkpoint boundary,
   restore its machine, and replay only committed entries after index 3.
7. Compare the complete canonical checkpoint image with full replay through index 7,
   including all successful retry results. Both reach revision 3. The old drain retry still
   returns revision 1 without rewinding current state; altered fields under that ID conflict.

The demo also rejects a corrupted checkpoint. Its transcript includes source/working-tree
state, Go/platform, checkpoint size/hash, log entries, positions, states, and assertions.

## Verification and next steps

Tests cover deterministic bytes, historical retries, input/output ownership, all truncation
points of a fixture, header/payload corruption, extra bytes, unknown versions/fields,
duplicate IDs/keys, reordered/gapped revisions, inconsistent state, invalid positions, and
size/request bounds. Twenty seeded histories compare full execution with repeated
checkpoint restoration plus continued execution, including successes, conflicts, and retries.

The serving gateway, controller, and live Raft recovery path retain their previous behavior.
The demo creates a new offline file; it does not implement atomic installed-checkpoint
replacement, directory durability, concurrent checkpoint capture, snapshot transfer, or
power-loss guarantees. The existing log capacity limit remains.

Next implementation sequence:

1. Define trusted capture and crash-safe checkpoint publication/recovery selection. Persist
   an entire checkpoint before publishing a reference; an orphan or partial file must not
   displace the prior recoverable state. Preserve current Raft term/vote independently.
2. Add offset-aware Raft log storage and recovery, retaining the included index/term. Remove
   prefix entries only after a durable checkpoint and suffix recovery contract exist.
3. Implement and fault-test lagging-peer snapshot installation before enabling compaction
   in the networked group. A peer behind the removed prefix must have a recovery path.
4. Specify successful-ID retention/capacity explicitly. Never claim compaction makes the
   application request table constant-size or silently weaken acknowledged retry behavior.
