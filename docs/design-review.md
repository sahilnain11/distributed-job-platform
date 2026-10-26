# Design Review

Initial review: 2026-10-16. Decisions completed: 2026-10-19. Source: [design.md](design.md).

**Status: design review and decision discussion complete.** All nine reviewed findings and the five owner questions are resolved. Local milestones 1–8 have since been implemented/validated, including reliability, observability, load/outage experiments, measured worker scaling and container packaging. Kubernetes/HA remain deferred. See [progress.md](progress.md) for exact evidence and limits. Review completion alone is not proof of an implemented guarantee.

## Understanding of the design

1. The API commits the job, payload, idempotency record, and outbox event together before returning `202`.
2. An outbox publisher sends a job reference to RabbitMQ. PostgreSQL remains the source of truth; RabbitMQ provides execution notifications and buffering.
3. A worker atomically claims a queued job and creates a distinct execution attempt.
4. Heartbeats renew that attempt's lease. Completion and failure updates must prove ownership.
5. Expired attempts are recovered; retryable failures wait in PostgreSQL until the retry scheduler queues them through a new outbox event.
6. The worker commits the execution outcome before acknowledging its message.
7. Delivery and execution are at-least-once. Attempt ownership protects platform state; downstream idempotency protects business side effects.

This is a coherent architecture for the learning goals. The job/attempt distinction, transactional outbox, explicit SQL, and separate business idempotency are particularly good choices. No technology replacement or additional infrastructure is needed to address the findings below.

## Priority legend

- **High:** resolve before implementing the affected correctness path.
- **Medium:** specify before operational testing or claiming the corresponding guarantee.

These are gaps in the written contract, not evidence of bugs in code that does not yet exist.

## R1 — Define when an outbox publication is successful

**High · Sections 9 and 11**

**Decision (2026-10-16):** approved durable topology, persistent messages, publisher confirms, routing checks, and marking only the specific outbox event published after confirmed success. Uncertain outcomes remain eligible for retry, accepting possible duplicates. Incorporated into Sections 9.1 and 11.1 of [design.md](design.md). The failure-scope follow-up was resolved on 2026-10-19: single-machine learning deployment, saved data must survive; permanent storage loss/corruption and redundant-node high availability are outside V1. See Section 1.1 and R9 below.

Calling a client library's publish method is not sufficient evidence that a message is safely queued. A connection failure can leave the outcome unknown; an unroutable message can also fail to reach the intended queue.

**Failure example:** mark an outbox row published after a local send; the connection drops before broker acceptance. The job remains `QUEUED`, but no message exists and the outbox no longer retries it.

**Approved publication and failure-scope contract:**

- Declare durable topology and persistent messages.
- Require publisher confirms and verify routing, for example using mandatory publishing with returned-message handling.
- Mark only the corresponding `outbox_id` published after a positive confirmation with no routing failure. There can be multiple outbox records for one job across retries.
- On a negative confirmation, return, or unknown outcome, leave the event eligible for retry. Duplicate sends are expected.
- V1 excludes permanent broker/database storage loss or corruption. Normal restart recovery assumes saved data survives and services return; persistent volumes are not replication or backups.

An outbox does not, by itself, repair a message lost after its row was marked published. Durable PostgreSQL commit assumptions also belong in the durability contract.

## R2 — Transactions need concurrency-safe ownership decisions

**High · Sections 12–13, 17, 19–20**

**Decision (2026-10-16):** approved short job-row locks: lock, check current ownership/state, save related changes together, and commit. Completion, failure handling, and recovery follow the same rule. Never hold the lock while a handler executes. The atomic claim includes attempt creation; the scheduler rechecks eligibility under its lock before queuing. Incorporated into Sections 13, 17, 19, and 20.1 of [design.md](design.md). The lease boundary is defined by the approved R3 decision below.

**Original gap:** the design required transactions for recovery and completion but did not explain how competing transactions serialize. A plain `SELECT` ownership check followed by writes is not sufficient under normal read-committed isolation.

**Failure example:** worker A reads that it owns the job; recovery expires A and commits; A then inserts a result and updates its attempt based on that earlier read. A transaction alone does not make its read an exclusive ownership claim.

**Approved approach:**

- Commit the `QUEUED → RUNNING` claim, counter increment, lease, and attempt insertion in one transaction.
- Use a job-row lock and a fresh state/ownership check before completion, failure, recovery, and retry scheduling. The conditional claim update already acquires its row lock; its affected-row count gates attempt creation.
- Commit result insertion, attempt completion, and job completion together. If ownership is lost, write none of them; roll back any preliminary changes.
- Apply the same ownership discipline to failure handling, not just success.
- Recovery must recheck the current attempt and expired lease while holding ownership of the transition; update the attempt and job together.
- The scheduler must conditionally move each due job to `QUEUED` and insert its outbox event together. Multiple scheduler instances must not independently schedule the same transition.
- Keep transactions short; never hold a database transaction open while a job handler executes.

`FOR UPDATE SKIP LOCKED` is one possible batching technique for pollers, not a requirement to add a distributed lock service.

## R3 — Decide whether lease expiry alone revokes ownership

**High · Sections 14–17 and 20**

**Decision (2026-10-16):** reject completion when the lease has expired at the ownership check, even if recovery has not run. Apply the same permission boundary to heartbeat renewal and worker-reported failure. Lock first, then check fresh database time using `clock_timestamp()`. Equality counts as expired; recovery uses `<=`. Check time, not commit time, is the boundary. Incorporated into Sections 14.1–17 and 20.2 of [design.md](design.md).

**Original gap:** heartbeat renewal checked the lease deadline, but completion only checked status and attempt ID. An expired worker could therefore complete during the interval before recovery ran.

Two policies are possible:

1. **Strict expiry:** an expired lease immediately invalidates completion/failure authority, even before recovery observes it.
2. **Recovery-mediated expiry:** completion can still win if recovery has not yet revoked the attempt.

**Approved rule:** use strict expiry consistently for heartbeat, success, and failure updates, with fresh database time checked after locking. Recovery considers `lease_expiry <= current database time`.

Also make one limitation explicit: a paused worker can resume after replacement work starts. An attempt UUID guards database updates; it cannot forcibly stop old code or fence an external system. Handlers should stop cooperatively on ownership loss, and side-effecting handlers still need stable business idempotency keys.

## R4 — The idempotency schema only models single-job submission

**High · Sections 5.1–5.2, 5.5, and 8.5**

**Decision (2026-10-16):** validate all batch items, atomically accept valid ones with their payloads/outbox events and the per-item response, and replay the exact original outcome for matching requests. Use a request-outcome record with response status/body and a database-enforced scoped key rather than one `job_id`. The server assigns `client_id = 'local-client'` to every V1 request; no caller-supplied client identity or login system is required. Approved server-built scopes are `SUBMIT_JOB`, `SUBMIT_BATCH`, and `RETRY_JOB:<canonical-job-uuid>`. Fingerprint the whole parsed JSON request using consistent normalization and SHA-256: ignore formatting and object-field order; preserve values, types, and array order. Keep records indefinitely in V1, without automatic expiry or cleanup; revisit retention later. Batch responses use counts and ordered, indexed per-item outcomes; return `202` if any job is accepted and `400` if all items are invalid. Persist and replay properly structured all-invalid batch outcomes too. Malformed JSON or invalid batch structure returns `400` without accepting jobs. Incorporated into Sections 5.2, 8.5–8.5.4, and 10 of [design.md](design.md). R4's reviewed decisions are complete; implementation and tests have not started.

**Original gap:** the table pointed to one `job_id` and omitted the proposed `client_id` column. It could not represent a batch-wide response containing several job IDs and validation failures, or distinguish submission from retry operations.

**Failure example:** the first batch request commits accepted items and times out before the client receives the response. A repeated request needs exactly those item outcomes and job IDs, not a new validation result or additional jobs.

**Approved core approach:**

- Store the response status and body in a request-outcome record keyed by `(client_id, operation_scope, idempotency_key)`.
- Use the fixed server-assigned `local-client` identity in V1. All callers share the namespace; this is not authentication or tenant isolation.
- Build scopes server-side: `SUBMIT_JOB`, `SUBMIT_BATCH`, and `RETRY_JOB:<canonical-job-uuid>`. Retry lookup is specific to the target job; different keys do not bypass job-state checks.
- Handle simultaneous uses of the same key with a unique constraint and transactional replay, not only a check-then-insert in application code.
- Replay matching requests and reject different requests using the same scoped key.
- Compare SHA-256 fingerprints of normalized JSON, not raw body bytes. Ignore formatting/object-field order; preserve values, JSON types, string contents, missing-versus-null distinctions, and array order. Include every batch item, even invalid ones.
- Retain idempotency records indefinitely in V1, including their fingerprints and original responses after jobs finish. No automatic expiry or cleanup; revisit this policy later.
- Validate all batch items first, then commit all valid jobs, payloads, outbox events, and the per-item response record in one transaction. Database failure rolls that entire acceptance transaction back. Invalid items remain rejected in the replayed response.
- Return `acceptedCount`, `rejectedCount`, and ordered `items` with zero-based indexes. Accepted items have job ID/status; rejected items have error code/message. Use `202` if any job is accepted; use `400` for all-invalid batches, malformed JSON, or invalid batch structure. Persist properly structured all-invalid outcomes for replay; changed input requires a new key.

Partial validation acceptance does not require partially committed database transactions.

**Status:** agreed and incorporated. Manual retry-cycle accounting is defined by the approved R5 decision below.

## R5 — Manual retry needs a persisted retry-cycle model

**High · Sections 5.5, 8.1, 8.3, and 18**

**Final decision (2026-10-16):** return `409 Conflict` when a new retry request targets an existing job not currently `FAILED`, including a different-key request that finds the job already queued by a competing request. Leave job/attempt state unchanged and create no outbox event for that rejected request. Matching saved responses take priority over current-state eligibility checks, including for simultaneous same-key requests. Incorporated into Section 5.5 of [design.md](design.md). R5's reviewed decisions are complete; implementation and tests have not started.

**Additional decision (2026-10-16):** allow multiple intentional manual retry cycles for one job. Each new cycle uses a fresh key and requires the job to be `FAILED`. Old matching keys replay their saved outcomes, even after the job fails again, and never restart work. There is no one-key-per-job rule or lifetime manual-cycle limit in V1. Incorporated into Section 5.5 of [design.md](design.md).

**Core decision (2026-10-16):** an accepted manual retry grants a fresh four-attempt cycle. Keep monotonic lifetime `attempt_count` and add `cycle_attempt_count`, initially zero and reset on manual retry acceptance. A committed worker claim increments both and creates the historical attempt; HTTP acceptance creates no attempt. Queue the job with an outbox event and idempotency outcome transactionally; clear completion, retry, and lease timestamps. Incorporated into Sections 5.5, 8.1, 13, 17, and 18.1 of [design.md](design.md). The later approved R8 decision also clears the job start timestamp and defines history/failure-summary meanings.

The design gives each automatic cycle four total attempts, while manual retry retains the job ID. A lifetime `attempt_count` alone cannot express both a fresh automatic retry budget and monotonically numbered historical attempts.

**Failure example:** after four failed attempts, a manual retry either immediately exhausts the unchanged counter or resets it and conflicts with `UNIQUE(job_id, attempt_number)`.

**Approved core approach:**

- Keep lifetime attempt numbers monotonic.
- Persist `cycle_attempt_count` for the current four-attempt budget and backoff calculation. Never reset lifetime `attempt_count`.
- An accepted manual retry conditionally transitions `FAILED → QUEUED` and creates an outbox event in the same transaction as its idempotency outcome.
- Create the execution attempt when a worker actually claims the job, not when the HTTP request is accepted.
- Grant a fresh four-attempt cycle on accepted manual retry; clear `completed_at`, `next_retry_at`, and `lease_expiry`.
- Allow multiple manual cycles over a job's lifetime, using a new key for each intended cycle. Replay a saved matching request before checking current eligibility; an old key never restarts work.

**Approved response rule:** only a successful `FAILED → QUEUED` transition accepts new work; return `409 Conflict` for a new request targeting an existing job in an ineligible state. A matching same-key replay returns the original response rather than this conflict.

Retryable handler failures and expired attempts must consume the same defined cycle budget. Infrastructure errors before an execution claim should not silently become handler attempts.

## R6 — Specify acknowledgements for every worker outcome

**High · Sections 12–13 and 18–20**

**Decision (2026-10-16):** for an owned success, retryable failure, or permanent/exhausted failure, commit the full outcome transaction before ACK. Retryable failure records a retry time and acknowledges the old delivery; the scheduler later produces a new notification through the outbox. ACK confirmed duplicate/stale deliveries without executing or writing as an old owner; database state must establish that the delivery no longer owns work. Recovery/scheduling owns further progress. For database failures or uncertain commits, reconnect and inspect the original attempt/current job before acting; retry persistence only with valid ownership, never blindly rerun the handler or ACK. Pause intake during outages and use capped infrastructure backoff rather than an immediate requeue loop. Reject malformed or authoritatively confirmed missing-job notifications without requeue, after structured logging and an error metric; no dead-letter queue in V1, so these deliveries are discarded. For a known queued job with a genuinely unsupported type, claim it and commit a permanent job/attempt failure before ACK. Incompatible worker versions are deployment faults, not invalid jobs. Incorporated into Sections 12.1–12.4 and 13 of [design.md](design.md). R6's reviewed decisions are complete; implementation and tests have not started.

**Original gap:** success ordering was already correct and zero-row claims were explicitly acknowledged, but the other worker paths also needed defined behavior. The agreed rules are below.

| Outcome | Agreed or proposed behavior |
| --- | --- |
| Owned success, retryable failure, or permanent failure | **Agreed:** commit the full corresponding database transition, then ACK. A scheduled retry gets a new notification through the outbox. |
| Duplicate or stale delivery, or ownership already lost | **Agreed:** do not execute/write as the old owner; ACK once the database authoritatively establishes that this delivery no longer owns work. Recovery/scheduling owns subsequent progress. A queued notification still requires a normal claim. |
| Database unavailable or commit outcome unknown | **Agreed:** reconnect and inspect authoritative state; ACK confirmed saved outcomes, retry saving only under valid ownership, otherwise follow stale-worker handling. Pause intake during outages and retry database access with capped backoff; leave unresolved deliveries unacknowledged. Do not blindly rerun handlers or hot-requeue messages. |
| Malformed message or confirmed missing-job reference | **Agreed:** structured error logging and metric, then reject without requeue. No V1 dead-letter queue; explicit discard trade-off. Database errors are not proof of a missing job. |
| Known job with genuinely unsupported type | **Agreed:** claim normally; commit permanent job/attempt failure under valid ownership, then ACK. Do not mistake worker version incompatibility for invalid work. |

Lease expiry and business failure are not the same thing as a broker redelivery. Keeping those paths distinct prevents accidental extra retry loops.

**Status:** agreed and incorporated. Runtime and worker limits are resolved under R7 below.

## R7 — Bound runtime separately from lease duration

**Medium · Sections 14, 16, and 24**

**Runtime decision (2026-10-16):** start with a configurable five-minute limit per attempt, measured from the attempt's own start time. On timeout, request cooperative cancellation and stop renewing that attempt's lease. Record `TIMEOUT` as retryable using the normal ownership/lease checks and cycle budget; if ownership has expired, recovery handles it. Cancellation is not proof that a stuck handler stopped, and external calls need timeouts too. Incorporated into Sections 14.2 and 18 of [design.md](design.md).

**Additional decision (2026-10-17):** bound initial worker-wide intake to ten outstanding deliveries for ten execution slots, with a bounded local backlog. Multiple consumers must share that aggregate allowance. Enforce actual capacity even if a delivery is acknowledged before an uncooperative handler exits; cancellation alone does not free a slot. Run heartbeats and timeout checks outside the handler pool and protect their database access from handler starvation. Incorporated into Sections 24.1–24.2 of [design.md](design.md).

**Final decision (2026-10-19):** use an initial configurable ten-minute broker consumer acknowledgement timeout (`consumer_timeout = 600000`). It starts at delivery, includes waiting/persistence margin, and is not extended by database heartbeats. Broker timeout/redelivery follows the existing ownership and reconciliation rules. Revisit the setting when runtime/intake assumptions change, and verify effective support during implementation. Incorporated into Section 24.3 of [design.md](design.md). R7's reviewed decisions are complete; implementation and tests have not started.

- A healthy job can run much longer than its lease duration because heartbeats renew the lease. No requirement should restrict a job to the initial 30-second lease.
- **Resolved:** a hung handler must not be intentionally heartbeated forever; use Section 14.2's runtime limit and timeout handling.
- **Resolved:** use the initial ten-minute broker acknowledgement timeout, separate from the database lease and five-minute runtime, with margin for local waiting and persistence.
- **Resolved:** bound worker-wide prefetch and local backlog against actual handler capacity; initially ten outstanding deliveries per ten-slot worker, not ten per consumer.
- **Resolved:** separate heartbeat/timeout scheduling from handler execution and ensure handlers cannot exhaust the database access heartbeats need.

These are operational decisions, not reasons to add another queue or scheduling system.

**Status:** agreed and incorporated. Payload and schema/lifecycle rules are resolved under R8 below.

## R8 — Tighten a few schema and lifecycle invariants

**Medium · Sections 6 and 8**

**Payload decision (2026-10-19):** remove `jobs.payload_id`. Use only `job_payloads.job_id`, required, unique, and referencing `jobs.job_id`. Require payload data and its creation time; create job and payload atomically. This enforces at most one payload row per job at the database level; the platform creation transaction supplies the guarantee that accepted jobs have a payload. Incorporated into Sections 8.1–8.2 of [design.md](design.md).

**Attempt constraint decision (2026-10-19):** require attempt job, worker, status, positive attempt number, and start time; enforce allowed attempt statuses with a CHECK constraint. Keep the unique lifetime attempt number per job and add a partial unique index allowing at most one recorded `RUNNING` attempt per job. `ended_at` remains nullable for active attempts. Incorporated into Section 8.3 of [design.md](design.md).

**Timestamp/history decision (2026-10-19):** keep job creation time unchanged; job start means the latest attempt start within the current cycle. Automatic retry waiting retains that start until the next claim; manual retry clears it. Job completion time marks either terminal success or terminal failure and is cleared on manual retry. Preserve individual attempt timestamps/errors as history, not as a new running/completed job's error. Incorporated into Sections 5.5, 8.1.1, 8.3, 12.1, 13, and 17 of [design.md](design.md).

**Final polling/retention decision (2026-10-19):** use configurable candidate batches, initially 100 per poll for publishing, retry scheduling, and recovery. Add the partial outbox index on `(created_at, outbox_id) WHERE published = false`; candidate reads do not grant exclusive ownership. Retain jobs, payloads, attempts/errors, results, and published outbox events without automatic cleanup for the initial learning version. Revisit retention before sustained load testing or longer-term deployment; published events are retained, not republished. Incorporated into Sections 8.6.1–8.7 of [design.md](design.md). R8's reviewed decisions are complete; implementation and tests have not started.

- **Resolved:** use one payload association through `job_payloads.job_id`, with atomic creation; no redundant pointer on `jobs`.
- **Resolved:** required attempt fields, valid attempt statuses, positive attempt numbers, and the partial unique running-attempt index supplement the already agreed counter checks and uniqueness rules.
- **Resolved:** latest start within the current cycle, completion time for both terminal outcomes, manual-retry reset, and preserved per-attempt timestamp/error history.
- **Resolved:** initial 100-candidate configurable poll batches, unpublished-outbox index, and no automatic cleanup of other records yet. Indefinite idempotency retention remains unchanged.

**Status:** agreed and incorporated. R9's remaining measurement/phase-boundary details were resolved below.

## R9 — Separate learning targets from demonstrated guarantees

**Medium · Sections 23–25**

**Failure-scope decision (2026-10-19):** V1 is a single-machine Docker Compose deployment with persistent PostgreSQL/RabbitMQ volumes. Recover after crashes, restarts, and temporary dependency outages if durable saved data survives and services return. Permanent storage loss/corruption, deleted volumes, and redundant-node high availability are not covered. Host outages imply platform downtime; the 99.9% target is not a V1 guarantee. Incorporated into Sections 1.1, 2, 11.1, and 23 of [design.md](design.md). This resolves R1's remaining failure-scope question.

**Handler decision (2026-10-19):** start with `ECHO` returning the submitted payload, and `SIMULATED_WORK` with a cancellable `durationMillis` wait and retryable failures for the first `failFirstAttempts` in the current cycle. The platform schedules retries; the handlers do not perform real external side effects. These cover the happy path, retry timing/exhaustion, runtime limits, and worker crash experiments. Incorporated into Sections 5.1 and 5.6 of [design.md](design.md).

**Final measurement/phase decision (2026-10-19):** retain the original performance numbers as unproven learning targets. Separately measure client-observed acceptance latency, acceptance-commit-to-first-start delay, individual attempts, newly accepted jobs/second, completed jobs/second, and backlog/errors. Baseline with fixed documented `ECHO` payloads and one-second no-planned-failure `SIMULATED_WORK`; record workload, sizes, hardware/resources, workers, settings, and duration, with failure scenarios labelled separately. Phase 4 must introduce atomic claim/attempt creation, initial lease/ownership checks, transactional outcome writes, commit-before-ACK, and duplicate protection together. Phase 5 extends this with retries, renewal, and recovery; Phase 4 alone is not reliable V1. Incorporated into Sections 23–23.1 and 25 of [design.md](design.md). R9's reviewed decisions are complete; implementation and tests have not started.

- **Resolved:** separate acceptance latency, first-start delay, attempt duration, and submission/completion throughput with explicit timing boundaries.
- **Resolved:** reproducible initial workloads and run descriptions, including backlog growth and separate failure scenarios.
- **Resolved:** use capacity estimates only as guidance; ten worker slots do not demonstrate the 1,000-submissions/second target or sustainable execution at that rate.
- **Resolved:** availability/performance targets are not demonstrated guarantees, and permanent data-loss recovery is outside V1.
- **Resolved:** correct claim/attempt/ownership/outcome/ACK behavior and duplicate tests belong in Phase 4; later reliability features extend that baseline.

## Questions for the project owner

1. **Batch behavior — resolved:** validate all items, atomically accept valid ones with the response record, and replay the original per-item outcome for a matching request. R4's client identity, scopes, fingerprinting, indefinite retention, and batch response policy are also agreed.
2. **Manual retry — resolved:** allow multiple fresh four-attempt cycles with new keys, preserving lifetime history. Replay matching saved requests; return `409` for a new request targeting an existing job not currently failed. See R5.
3. **Lease boundary — resolved:** reject expired-worker completion even if recovery has not run, using fresh database-time checks after locking. See R3.
4. **Initial handlers — resolved:** `ECHO` and `SIMULATED_WORK`, no real external side effects; five-minute per-attempt limit and explicit timeout tests. Benchmark measurement/reporting rules are agreed under R9.
5. **V1 failure scope — resolved:** single-machine learning deployment; recover from crashes/restarts/transient outages with durable saved data intact, not permanent storage loss/corruption or host-level high availability. See R9 and design Section 1.1.

## Review Outcome and Next Step

The nine reviewed findings and five owner questions are resolved. The source of truth is the updated [design.md](design.md); this document retains the review rationale and decision history. Exact framework/broker versions, serializer selection, pool sizing, and test tooling are implementation details to select and validate when their milestone is requested, not evidence that implementation exists.

Implementation proceeds only through requested milestones. Local milestones 1–8 are complete under their recorded validation scope; Kubernetes, multi-host HA and long-soak/capacity exploration remain unimplemented. The measured local results are not production capacity/availability guarantees. Do not implement later exploration merely because the design is approved. Track current implementation separately in [progress.md](progress.md), with tests for each implemented behavior.