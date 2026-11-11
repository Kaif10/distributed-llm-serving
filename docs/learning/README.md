# Walkthroughs and design notes

- [Review follow-up: bounded uploads and checkpoint-capacity admission](28-review-admission.md)

Each increment explains the problem, implementation, runnable checks, and limitations.
Start with the [project overview](../../README.md), or follow the walkthroughs in order.

- [Milestones](../PLAN.md): what to build, why, and what evidence completes each step.
- [Architecture](../ARCHITECTURE.md): request flow, control-plane responsibilities, and failure contracts.
- [First walkthrough](00-service.md): run the service and understand its lifecycle.
- [Streaming worker walkthrough](01-streaming-worker.md): watch chunks arrive and disconnect the client.
- [Gateway forwarding walkthrough](02-gateway-streaming.md): stream across two connections and trace failures.
- [Real inference walkthrough](03-real-inference.md): run pinned CPU inference and observe engine cancellation.
- [Worker pool walkthrough](04-worker-pool.md): run two real engines and reject excess work.
- [Health and draining walkthrough](05-health-and-draining.md): remove workers safely from new assignments.
- [Cache baseline walkthrough](06-cache-baseline.md): measure real prefix reuse and existing routing policies.
- [Prefix affinity walkthrough](07-prefix-affinity.md): preserve locality with capacity escape and real-model checks.
- [Load and goodput walkthrough](08-load-and-goodput.md): compare fixed arrivals and changing hot prefixes.
- [Mixed request costs](09-mixed-costs.md): measure short/long generations and different CPU allocations.
- [Restart under load](10-restart-under-load.md): verify real worker recovery while arrivals continue.
- [Decisions](../DECISIONS.md): choices and alternatives, including where Raft belongs.
- [Durable configuration](11-durable-configuration.md): journal recovery and revision contracts.
- [Applied configuration](12-applied-configuration.md): distinguish saved, applied, and drained state.
- [Deterministic state machine](13-state-machine.md): separate ordered decisions from journal I/O.
- [Raft election core](14-raft-elections.md): durable voting and deterministic election/failure scenarios.
- [Raft log replication](15-raft-replication.md): durable proposals, conflict repair, and quorum commit.
- [Raft peer processes](16-raft-processes.md): bounded HTTP transport, leader loss, and journal recovery.
- [Replicated configuration](17-replicated-configuration.md): commit/apply, retry replay, and the initial read contract.
- [Read quorums](18-read-quorum.md): fresh reads without per-poll journal growth.
- [Controller failover](19-controller-failover.md): bounded peer polling, stale-view expiry, and real-model recovery.
- [Application checkpoints](20-application-checkpoints.md): preserve retry results before log retention.
- [Checkpoint publication](21-checkpoint-publication.md): sync-before-selection, process-kill recovery, and uncertain retries.
- [Controller checkpoint recovery](22-controller-checkpoint-recovery.md): owner capture, confirmed-prefix installation, and full-replay comparison.
- [Controller capacity](23-controller-capacity.md): measured write limits, journal growth, and restart failure at entry exhaustion.
- [Retained log boundary](24-retained-log-boundary.md): absolute Raft indices and catch-up requirements.
- [Retained storage](25-retained-storage.md): two bounded file slots recover a checkpoint and suffix together, with native publication checks.
- [Controller local retention](26-controller-local-retention.md): opt-in single-member service recovery, manual retention, and restart/retry checks.
- [Peer snapshot recovery](27-peer-snapshot-recovery.md): three-controller retention, lagging-peer installation, and real-model quorum recovery.
- [Recorded demo](../demo/README.md): real-model routing/cancellation, controller snapshots, failure/recovery, and the complete capture evidence.
- [Evidence](../EVIDENCE.md): implemented behavior and validation records.
