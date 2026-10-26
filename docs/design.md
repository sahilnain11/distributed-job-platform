# Distributed Job Processing Platform

## 1. Overview

A reliable distributed job processing platform that allows applications to submit asynchronous jobs and have them executed by a scalable pool of workers.

The platform is responsible for:

- Accepting jobs reliably
- Persisting job state
- Queuing jobs for asynchronous execution
- Executing jobs using a worker pool
- Tracking job lifecycle and execution attempts
- Retrying retryable failures
- Detecting failed workers
- Recovering jobs from worker failures
- Preventing duplicate client submissions
- Handling duplicate message delivery safely
- Storing job results
- Providing job status and execution history

The platform provides **at-least-once execution semantics**. Exactly-once execution is not assumed.

## 1.1 V1 Deployment and Failure Scope

Start with a single-machine Docker Compose learning deployment. PostgreSQL and RabbitMQ use persistent volumes that are reused across ordinary service/container and host restarts.

V1 is intended to recover from process crashes, service restarts, temporary dependency outages, and host restarts **provided durable saved data survives and the required services return**. This means recovery of accepted work, not uninterrupted availability or guaranteed job success: retry budgets and permanent failures still apply.

| Situation | V1 expectation |
| --- | --- |
| API or worker crashes | Recover using committed job/outbox state, message delivery, leases, and retry rules |
| PostgreSQL or RabbitMQ temporarily unavailable | Pause/retry safely and resume after service recovery |
| Services or host restart with storage intact | Recover after required services are running again |
| Entire machine unavailable | Platform is unavailable until the machine/services return |
| Permanent storage loss, corruption, or deleted data volumes | Not covered by V1 recovery guarantees |

Persistent volumes are not backups or replication. Permanent-data-loss recovery, redundant-node high availability, and associated recovery procedures are later exploration, not promises made by this deployment. Do not delete/recreate volumes and expect restart recovery to restore their data.

Durability assumes PostgreSQL's durable commits and RabbitMQ's durable topology/persistent message guarantees are enabled and their storage remains intact. The outbox does not restore a previously confirmed/published notification if the broker's saved data is permanently destroyed; losing PostgreSQL storage also loses job state and idempotency history.

---

# 2. Goals

### Primary goals

1. Demonstrate distributed systems concepts.
2. Provide reliable asynchronous job execution.
3. Support horizontal worker scaling.
4. Handle worker crashes and message redelivery.
5. Implement idempotency.
6. Implement retries with exponential backoff.
7. Implement worker leases and heartbeats.
8. Provide observability into job execution.
9. Measure system throughput, latency, and bottlenecks.
10. Support failure injection and load testing.

### Non-goals for V1

- Multi-region deployment
- Exactly-once execution
- Complex workflow/DAG orchestration
- User-facing UI
- Multi-tenant billing
- Advanced scheduling/cron jobs
- Kafka-based event streaming
- Redundant-node high availability and recovery from permanent storage loss or corruption

These may be explored later.

---

# 3. Technology Stack

## Core

- Java 21
- Spring Boot
- Maven
- PostgreSQL
- JPA / Hibernate
- Explicit SQL for concurrency-critical operations
- Flyway
- RabbitMQ
- Docker

## Later

- Prometheus
- Grafana
- Kubernetes
- Load testing tooling
- Failure injection

---

# 4. High-Level Architecture

```text
                    ┌──────────────┐
                    │    Client    │
                    └──────┬───────┘
                           │
                           │ HTTP
                           ▼
                    ┌──────────────┐
                    │ API Server   │
                    │ Spring Boot  │
                    └──────┬───────┘
                           │
                           │
                           ▼
                    ┌──────────────┐
                    │ PostgreSQL   │
                    │              │
                    │ Job State    │
                    │ Payloads     │
                    │ Attempts     │
                    │ Results      │
                    │ Outbox       │
                    └──────┬───────┘
                           │
                    Outbox Publisher
                           │
                           ▼
                    ┌──────────────┐
                    │  RabbitMQ    │
                    │              │
                    │ Job Queue    │
                    └──────┬───────┘
                           │
                    ┌──────┴───────┐
                    │              │
                    ▼              ▼
              ┌──────────┐   ┌──────────┐
              │ Worker 1 │   │ Worker 2 │
              └────┬─────┘   └────┬─────┘
                   │              │
                   └──────┬───────┘
                          ▼
                    Job Handlers
```

---

# 5. Functional Requirements

## 5.1 Submit a Job

Clients can submit an asynchronous job.

```http
POST /jobs
Idempotency-Key: <client-generated-key>
```

Example request:

```json
{
   "type": "ECHO",
  "payload": {
      "message": "hello"
  }
}
```

The API returns:

```http
202 Accepted
```

```json
{
  "jobId": "<uuid>",
  "status": "QUEUED"
}
```

The API must not wait for job execution to finish.

Implemented request/response, validation, and idempotency details are documented in [api.md](api.md). Phase 4 introduced opt-in publishing and ECHO execution; Phase 5 now adds simulation/retries/lease renewal/runtime limits/recovery. See [messaging.md](messaging.md) and [reliability.md](reliability.md) for behavior and verified limits.

---

## 5.2 Submit Batch Jobs

Clients can submit multiple jobs.

```http
POST /jobs/batch
Idempotency-Key: <batch-idempotency-key>
```

Jobs within the batch are independent.

The implemented batch envelope is `{ "items": [<job-request>, ...] }`, with 1–100 items and no other envelope fields. Invalid individual job objects are per-item rejections; malformed/empty/oversized batch envelopes are HTTP 400 without acceptance.

Validate all items before accepting the batch. Accept valid items and reject invalid ones; an invalid item does not prevent valid items from being accepted.

The idempotency key applies to the **entire batch request**, not individual jobs.

Commit all valid jobs, their payloads and outbox events, and the complete per-item response record in one database transaction. If that transaction fails, none of those changes is accepted.

Repeating the same request with the same scoped key returns the stored response, including the original job IDs and rejected-item outcomes. It does not create new jobs or revalidate rejected items. Reusing that scoped key with different input is rejected.

### Batch Response

Return `acceptedCount`, `rejectedCount`, and an `items` array in the original request order. Each item includes its zero-based `index` and an `outcome` of `ACCEPTED` or `REJECTED`.

- An accepted item includes its `jobId` and the acceptance-time job status `QUEUED`.
- A rejected item includes an `error` with a structured `code` and human-readable `message`; it does not create a job.

Example response for a batch with one accepted and one rejected item:

```json
{
   "acceptedCount": 1,
   "rejectedCount": 1,
   "items": [
      {
         "index": 0,
         "outcome": "ACCEPTED",
         "jobId": "550e8400-e29b-41d4-a716-446655440000",
         "status": "QUEUED"
      },
      {
         "index": 1,
         "outcome": "REJECTED",
         "error": {
            "code": "INVALID_PAYLOAD",
            "message": "customerId is required"
         }
      }
   ]
}
```

HTTP status policy:

| Situation | HTTP status and outcome |
| --- | --- |
| At least one job accepted | `202 Accepted`, with every item's outcome |
| Every item invalid in a properly structured batch | `400 Bad Request`, with every item's rejection |
| Malformed JSON or invalid batch structure | `400 Bad Request`; no jobs accepted |

For a properly structured batch, persist the full response status/body and request fingerprint even when every item is rejected. That all-invalid outcome commits as an idempotency record without any jobs or outbox events. A matching replay returns the saved outcome, including its original HTTP status. Fixing rejected input requires a new idempotency key.

---

## 5.3 Get Job

```http
GET /jobs/{jobId}
```

Returns job information including:

- Job ID
- Job type
- Current status
- Creation time
- Start time
- Completion time
- Attempt count
- Failure information
- Result/reference when available

---

## 5.4 List Jobs

```http
GET /jobs?status=FAILED
```

The API should support filtering by status.

Phase 3 returns newest-first summaries with a bounded `limit` (default 100, range 1–100), without loading payloads. This initial endpoint is not a full pagination API; see [api.md](api.md).

Additional filtering/pagination may be added later.

---

## 5.5 Manually Retry a Failed Job

```http
POST /jobs/{jobId}/retry
Idempotency-Key: <key>
```

An accepted manual retry queues a fresh automatic retry cycle while retaining the same `job_id`. It grants up to four execution attempts: one initial attempt and three automatic retries for retryable failures.

Automatic retry limits apply to an automatic retry cycle. Manual retry is an explicit client operation.

Multiple manual retry cycles are allowed for the same job over its lifetime. V1 does not impose a lifetime limit on their number. Each new cycle requires the job to be `FAILED` and a new idempotency key within that job's retry scope.

An idempotency key identifies a request, not the job itself. A job can therefore have multiple saved retry-request records. Reusing an old matching key only replays that request's saved response, even if the job has failed again; it never starts another cycle. Look for a saved matching request before checking eligibility for new work.

Example:

| Request for the same job | Outcome |
| --- | --- |
| `retry-A` while the job is failed | Accept a new four-attempt cycle |
| Repeat `retry-A` after that cycle fails | Replay A's original response; do not start work |
| New key `retry-B` while the job is failed again | Accept another four-attempt cycle, preserving history |

Use separate lifetime and current-cycle counters as defined in Sections 8.1 and 18.1. Do not reset historical attempt numbers. Create the new attempt only when a worker claims the queued job, not when the retry request is accepted.

The accepted operation conditionally changes `FAILED → QUEUED`, resets `cycle_attempt_count` to zero, and clears `started_at`, `completed_at`, `next_retry_at`, and `lease_expiry`. Leave `created_at`, `attempt_count`, and historical attempts unchanged. Commit the job change, new outbox event, and idempotency response in one transaction.

### Retry Eligibility and Request Replay

For an existing job:

| Request | Outcome |
| --- | --- |
| Saved scoped key with matching input | Replay the saved response before checking the current job state; do not create a cycle |
| Saved scoped key with different input | Reject key reuse; do not create a cycle |
| New scoped key and job currently `FAILED` | Accept a fresh retry cycle |
| New scoped key and job currently `QUEUED`, `RUNNING`, `RETRYING`, or `COMPLETED` | Return `409 Conflict`: the job is not currently failed |

For a new request, check eligibility under the job-row lock and conditionally perform the transition. The job-row update is:

```sql
UPDATE jobs
SET status = 'QUEUED',
      cycle_attempt_count = 0,
   started_at = NULL,
      completed_at = NULL,
      next_retry_at = NULL,
      lease_expiry = NULL
WHERE job_id = :job_id
   AND status = 'FAILED';
```

Only a successful transition may create the new outbox event and acceptance response, all within the same transaction. If the existing job is ineligible, leave its counters, state, and attempts unchanged and create no outbox event for that request.

Competing updates to the same job take turns. If one request has changed the job to `QUEUED`, a competing different-key request that sees this state returns `409` instead of resetting the budget again. A new key does not bypass the eligibility check.

Simultaneous uses of the same scoped key follow Section 8.5's uniqueness and transactional replay rules. A request racing with another use of its key must resolve the saved outcome before returning an eligibility conflict; it must not mistake a repeat of an accepted request for new work.

## 5.6 Initial Learning Handlers

Start with `ECHO` and `SIMULATED_WORK`. A handler performs the work for one job type; it reports success or failure, while the platform retains ownership of retry policy and scheduling.

### ECHO

Return the submitted payload as the job's result. For example:

```json
{
   "type": "ECHO",
   "payload": {
      "message": "hello"
   }
}
```

This exercises submission, persistence, queue delivery, execution, result storage, and status retrieval without an external dependency.

### SIMULATED_WORK

Use a cancellable wait to simulate work and predictable failures to exercise retries:

```json
{
   "type": "SIMULATED_WORK",
   "payload": {
      "durationMillis": 1000,
      "failFirstAttempts": 2
   }
}
```

- Each invocation waits for the requested `durationMillis`, subject to cancellation and the platform's runtime limit.
- The first `failFirstAttempts` claimed attempts in the **current cycle** report a retryable failure. Use the attempt's cycle position supplied by the platform, not mutable process-local counters.
- Later attempts report success after the wait. The example fails on cycle attempts 1 and 2, then succeeds on cycle attempt 3, with platform-managed backoff between them.
- An accepted manual retry resets the cycle position, so the same failure pattern applies in that new cycle. Historical lifetime attempt numbers do not reset.
- A zero failure count supports successful timing tests; settings covering the whole cycle exercise exhaustion. Long waits exercise timeout handling. Stopping a worker while work runs exercises lease-based crash recovery separately from simulated handler failure.

Validate simulation settings as nonnegative integer values and handle waits cooperatively; these learning controls must not bypass platform retry budgets, ownership checks, bounded execution capacity, or timeouts. No handler schedules its own retries.

Neither initial handler sends emails, processes payments, or calls a real external service. They provide a safe initial learning/testing workload, not proof of capacity for arbitrary business jobs or of downstream side-effect idempotency. Add those demonstrations separately when requested.

---

# 6. Job Lifecycle

Normal lifecycle:

```text
QUEUED
   │
   ▼
RUNNING
   │
   ▼
COMPLETED
```

Retryable failure:

```text
RUNNING
   │
   ▼
RETRYING
   │
   ▼
QUEUED
   │
   ▼
RUNNING
```

Permanent failure:

```text
RUNNING
   │
   ▼
FAILED
```

Worker failure:

```text
RUNNING
   │
   │ lease expires
   ▼
RETRYING
   │
   ▼
QUEUED
```

After retry exhaustion:

```text
RETRYING → FAILED
```

---

# 7. Job vs Attempt

A **job** represents the work requested by the client.

An **attempt** represents one execution of that job.

Example:

```text
Job J1

├── Attempt A1 → FAILED
├── Attempt A2 → EXPIRED
├── Attempt A3 → FAILED
└── Attempt A4 → COMPLETED
```

The `job_id` remains constant.

Every execution receives a new `attempt_id`.

This distinction is important for handling retries, worker failures, stale workers, and observability.

---

# 8. Database Design

## 8.1 jobs

```text
job_id              UUID PK
type                VARCHAR(255) NOT NULL
status              VARCHAR(255) NOT NULL DEFAULT 'QUEUED'
attempt_count       INT NOT NULL DEFAULT 0
cycle_attempt_count INT NOT NULL DEFAULT 0
attempt_id          UUID NULL
lease_expiry        TIMESTAMPTZ NULL
next_retry_at       TIMESTAMPTZ NULL
created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
started_at          TIMESTAMPTZ NULL
completed_at        TIMESTAMPTZ NULL
```

Valid statuses:

```text
QUEUED
RUNNING
RETRYING
COMPLETED
FAILED
```

The database should enforce the valid status values using a CHECK constraint.

Attempt counter meanings:

- `attempt_count` is the number of claimed attempts across the job's lifetime. Never reset it; the newly incremented value supplies `job_attempts.attempt_number`.
- `cycle_attempt_count` is the number of claimed attempts in the current automatic retry cycle. Initially zero, it resets to zero only when a new manual retry cycle is accepted.
- A successful claim increments both counters in the same transaction as attempt creation. Failed or rolled-back claims increment neither.
- Enforce `attempt_count >= 0`, `cycle_attempt_count BETWEEN 0 AND 4`, and `cycle_attempt_count <= attempt_count` with CHECK constraints.

Indexes:

```text
(status, next_retry_at)
(status, lease_expiry)
```

`job_id` is the primary key and therefore already indexed.

`attempt_id` is a pointer to the latest/current attempt. It is not required to have a foreign-key constraint to `job_attempts`.

There is no `jobs.payload_id` pointer. Retrieve the payload through the authoritative `job_payloads.job_id` relationship described below.

## 8.1.1 Timestamp Meanings Across Retries

| Field | Meaning |
| --- | --- |
| `jobs.created_at` | Original job creation time; never changed by retry |
| `jobs.started_at` | Latest claimed attempt's start time within the current retry cycle; null before that cycle's first claim |
| `jobs.completed_at` | Time the job enters terminal `COMPLETED` or `FAILED`; null while the job is queued, running, or waiting for retry |
| `job_attempts.started_at` | That attempt's claim/start time; preserved as history |
| `job_attempts.ended_at` | Time its terminal outcome is recorded; null while the attempt is `RUNNING` |

A successful claim replaces `jobs.started_at` with the new attempt's start time. Use the same database-derived start value for the job and its newly created attempt. Automatic retry waiting/queuing retains the preceding attempt's start time until the next claim replaces it.

Every attempt completion, failure, or recovery expiry records its `ended_at` in the same transaction as its status change. For recovery, this is the time expiry is recorded, not a claim to know exactly when the handler stopped. Per-attempt start/end timestamps remain available after subsequent cycles begin.

Set `jobs.completed_at` on both success and terminal failure, including failure from exhausted recovery attempts. Do not set it merely because an individual attempt failed if the job will retry. An accepted manual retry clears both job start/completion timestamps until new execution begins; it does not alter the original creation time or past attempt records.

Example:

```text
10:00 job created
10:01 attempt 1 starts
10:02 attempt 1 fails; automatic retry scheduled
10:03 attempt 2 starts
10:04 attempt 2 permanently fails

Job: created_at=10:00, started_at=10:03, completed_at=10:04, status=FAILED

10:05 manual retry accepted

Job: created_at=10:00, started_at=NULL, completed_at=NULL, status=QUEUED
```

---

# 8.2 job_payloads

```text
payload_id      UUID PK
job_id          UUID NOT NULL FK UNIQUE
payload         JSONB NOT NULL
created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

Constraints:

- `job_id` references `jobs.job_id`
- `job_id` is required and UNIQUE: each payload belongs to one existing job, and a job has at most one payload row.
- Create the job and its payload in the same transaction so the platform's creation path never commits a job without its payload. The foreign key and uniqueness constraint alone do not enforce that every job has a payload.
- Keep this association only in `job_payloads.job_id`; do not duplicate it with a pointer on `jobs`.

Conceptual table definition:

```sql
CREATE TABLE job_payloads (
   payload_id UUID PRIMARY KEY,
   job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id),
   payload JSONB NOT NULL,
   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Workers retrieve the payload directly by job ID:

```sql
SELECT payload
FROM job_payloads
WHERE job_id = :job_id;
```

Large payloads may be moved to object storage in a future version.

---

# 8.3 job_attempts

```text
attempt_id       UUID PK
job_id           UUID NOT NULL FK
attempt_number   INT NOT NULL
worker_id        UUID NOT NULL
status           VARCHAR NOT NULL
started_at       TIMESTAMPTZ NOT NULL
ended_at         TIMESTAMPTZ NULL
failure_type     VARCHAR NULL
failure_reason   TEXT NULL
```

Valid statuses:

```text
RUNNING
FAILED
COMPLETED
EXPIRED
```

Constraints:

```text
UNIQUE(job_id, attempt_number)
CHECK(attempt_number > 0)
CHECK(status IN ('RUNNING', 'FAILED', 'COMPLETED', 'EXPIRED'))
```

`job_id` references `jobs.job_id`.

Require `job_id`, `attempt_number`, `worker_id`, `status`, and `started_at` on every attempt. An active attempt has no end time yet, so `ended_at` remains nullable. Keep lifetime attempt numbering unique per job as described in Section 18.1.

Also enforce at most one recorded `RUNNING` attempt per job with a partial unique index:

```sql
CREATE UNIQUE INDEX one_running_attempt_per_job
ON job_attempts (job_id)
WHERE status = 'RUNNING';
```

This index applies only to rows marked `RUNNING`. It allows the job's earlier failed, completed, and expired attempts to remain in history. Recovery must commit the old attempt's `EXPIRED` transition with the job's recovery state before a new running attempt is created.

These constraints catch invalid database records even if application code has a bug. They do not replace atomic claim/ownership transactions, forcibly stop an old handler, or prevent external side effects.

`failure_type` is a structured category useful for metrics/filtering.

`failure_reason` contains detailed debugging information.

Preserve past attempt errors rather than erasing them during retry. Job retrieval must distinguish a historical failure from the current attempt's outcome: do not present an earlier error as a failure of a new running attempt or a successfully completed job. In particular, a manual retry with no new claimed attempt must not present the preceding cycle's failure as a new-cycle failure.

Attempt history can be retrieved with:

```sql
SELECT attempt_number, status, failure_type, failure_reason,
       started_at, ended_at
FROM job_attempts
WHERE job_id = :job_id
ORDER BY attempt_number;
```

---

# 8.4 job_results

```text
result_id       UUID PK
job_id          UUID NOT NULL FK UNIQUE
result          JSONB NULL
storage_type    VARCHAR(255) NOT NULL
location        TEXT NULL
created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
```

A job has at most one final result.

Small results may be stored in PostgreSQL.

Large results may be stored in object storage, with `location` containing the reference.

Example:

```text
storage_type = DATABASE
result = {...}
```

or:

```text
storage_type = S3
location = <object-storage-reference>
```

---

# 8.5 idempotency_keys

```text
client_id          TEXT NOT NULL
operation_scope    TEXT NOT NULL
idempotency_key    TEXT NOT NULL
request_hash       TEXT NOT NULL
response_status    INT NOT NULL
response_body      JSONB NOT NULL
created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

This table remembers the outcome of an API request, not just a reference to one job. `response_status` is the original HTTP status; `response_body` stores the response, including all accepted job IDs and rejected items for a batch.

Constraint:

```text
PRIMARY KEY (client_id, operation_scope, idempotency_key)
```

The key is scoped to the client and operation. Single submission, batch submission, and manual retry use the distinct operation scopes defined in Section 8.5.2. V1's fixed client identity is defined below.

`request_hash` is a fingerprint of the original request.

If the same scoped idempotency key is reused with a different request, reject it rather than returning the original outcome. If the request matches, replay the original response rather than reconstructing it from current job state.

The request-outcome record and all corresponding jobs, payloads, and outbox events commit together. The database primary key prevents simultaneous uses of the same scoped key from both committing new work. A transaction that loses that conflict must roll back its new work before reading and comparing the committed original record.

Request fingerprinting follows Section 8.5.3. V1 keeps idempotency records indefinitely as described in Section 8.5.4.

## 8.5.1 V1 Client Identity

For V1, the server assigns every API request the fixed `client_id = 'local-client'`. Do not require a client-ID header or introduce a login system for this local learning baseline.

All callers share this namespace. They must use different idempotency keys for unrelated requests within the same operation scope; a different caller does not automatically get a separate namespace.

This fixed value is not authentication or tenant isolation. If separate authenticated clients are added later, derive `client_id` from their authenticated identity rather than trusting an arbitrary caller-supplied ID.

## 8.5.2 Operation Scopes

The server builds `operation_scope` from the API operation and, for manual retry, its target job. Do not accept an arbitrary scope supplied by the caller.

| API operation | Scope |
| --- | --- |
| `POST /jobs` | `SUBMIT_JOB` |
| `POST /jobs/batch` | `SUBMIT_BATCH` |
| `POST /jobs/{jobId}/retry` | `RETRY_JOB:<job-id>` |

For manual retry, parse the job ID as a UUID and use its canonical lowercase, hyphenated text representation. Different text spellings of the same UUID must not create separate scopes.

Example lookup for a retry request (`:job_id` is a parsed UUID parameter):

```sql
SELECT request_hash, response_status, response_body
FROM idempotency_keys
WHERE client_id = 'local-client'
   AND operation_scope = 'RETRY_JOB:' || CAST(:job_id AS TEXT)
   AND idempotency_key = :key;
```

The same key can independently identify a single submission, a batch, and retries of different jobs. Within the same client and scope, a matching repeated request replays the saved outcome; changed input is rejected.

This lookup alone does not prevent concurrent duplicate requests. The primary key and transactional handling described above provide that protection. Using a different key also does not bypass the target job's retry eligibility checks; manual retry-cycle behavior remains an R5 decision.

## 8.5.3 Request Fingerprints

Calculate `request_hash` as SHA-256 of a consistent UTF-8 representation of the parsed JSON request. Do not hash the raw HTTP body bytes: formatting and object-field order do not change the meaning of the request.

The normalization rules are:

- Put object fields in a consistent order, including fields inside nested objects.
- Ignore insignificant JSON formatting whitespace, but preserve string contents. Do not trim strings or change their case.
- Keep array order unchanged, including batch item order. Reordering batch items counts as changed input.
- Preserve values and JSON types. The string `"123"` and the number `123` are different; an omitted field and an explicit `null` are different.
- Fingerprint the whole submitted batch, including invalid items, not just jobs that pass validation. Do not silently drop fields or apply business defaults before fingerprinting.

Use the same normalization and hash algorithm for initial requests and replays. The implementation must choose a deterministic JSON serializer/canonicalizer and test these rules, including numeric representation without precision loss; hashing must not rely on incidental object iteration order.

Conceptually (pseudocode):

```text
normalizedRequest = canonicalizeJson(requestBody)
newHash = sha256(utf8(normalizedRequest))

if savedHash == newHash:
   return savedResponse
else:
   reject "Idempotency key reused with different input"
```

The scope and client are matched separately as described above. The hash compares request content within that scope; it is not authentication or a substitute for the database uniqueness constraint.

Phase 3 implements a versioned `v1:` SHA-256 fingerprint, with arbitrary-precision numeric parsing and normalized numeric values (`1`, `1.0`, and `1e0` match; the string `"1"` differs). Duplicate object fields and trailing JSON values are rejected. JSONB storage/reload uses the same precision-preserving mapper. The complete API contract is in [api.md](api.md).

To serialize same-key submissions, the submission transaction first inserts a private, uncommitted request reservation with `ON CONFLICT DO NOTHING`. The database primary key makes a competing insert wait for the original transaction. After a conflict, a fresh read-committed statement checks the committed fingerprint/outcome. Newly reserved requests finalize their saved response in the same transaction as all job/payload/outbox writes; any failure rolls back the reservation and new work together. An incomplete reservation is never committed as an API response.

Replay returns the stored HTTP status and JSON content, not byte-identical object-key ordering. Well-formed invalid single requests and properly structured all-invalid batches save replayable 400 outcomes; malformed JSON, invalid envelopes, and missing/invalid request keys are rejected before reservation. Changed input under a saved scoped key returns 409.

## 8.5.4 V1 Record Retention

Keep idempotency records indefinitely for now. Do not add a time-to-live, automatic expiration, or a cleanup job for these records in V1.

Keep the request fingerprint and original response even after the jobs finish. Repeating a matching request still replays that saved outcome, regardless of its age. Use a new key for genuinely new work within the same client and operation scope.

This policy means the table will keep growing. Revisit retention later as an explicit design decision, together with the resulting limits on duplicate-request protection. Section 8.7 defines the initial no-cleanup policy for other records.

A deliberate development database reset or deletion of these records removes their protection; indefinite retention is not a guarantee against storage loss.

---

# 8.6 outbox

```text
outbox_id       UUID PK
job_id          UUID NOT NULL FK
published       BOOLEAN NOT NULL DEFAULT FALSE
created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
published_at    TIMESTAMPTZ NULL
```

The outbox represents:

> "This job needs to be published to the queue."

The outbox does not contain the complete job payload.

Each event references an existing job through `outbox.job_id`; it is not unique because retries can produce multiple events for the same job. Phase 2's migration makes the reference required and enforces the foreign key, preventing orphan notifications.

The publisher can retrieve the job and payload from PostgreSQL using `job_id`.

For V1, no explicit retry counter is required.

## 8.6.1 Bounded Polling and Outbox Index

Use configurable bounded candidate batches for the outbox publisher, Retry Scheduler, and Recovery Manager. Initially read at most **100 candidates per poll** rather than loading all eligible work into memory or processing an unbounded transaction.

For example, find unpublished outbox candidates in stable oldest-first order:

```sql
SELECT outbox_id, job_id
FROM outbox
WHERE published = false
ORDER BY created_at, outbox_id
LIMIT 100;
```

Use the configured batch size instead of a hard-coded limit in the implementation. Support this lookup with:

```sql
CREATE INDEX outbox_unpublished_idx
ON outbox (created_at, outbox_id)
WHERE published = false;
```

This partial index contains only unpublished events. Updating an event to published removes it from that index, not from the outbox table.

Candidate reads are not exclusive claims and do not publish anything. Existing publisher-confirm and duplicate-delivery rules still apply. Retry scheduling and recovery must recheck conditions under the job-row lock and commit related changes together. Their existing `(status, next_retry_at)` and `(status, lease_expiry)` indexes support due-work discovery. Keep polling transactions short and local work queues bounded; a candidate batch is not permission to hold locks across handler execution.

## 8.7 Initial Record Retention

For the local learning version, do not automatically expire or delete jobs, payloads, attempt history/errors, results, or published outbox events. Retain them for debugging and revisit cleanup later rather than adding retention machinery to V1 now. Section 8.5.4's indefinite idempotency retention remains unchanged.

Retaining a published outbox event does not make it eligible for republishing; normal publisher polling still selects only unpublished events. Failed publish outcomes remain eligible under Section 9.1.

This choice permits database growth. Revisit retention before sustained load testing or longer-term deployment, including how any later deletion affects job history, results, and saved idempotency responses. A deliberate development database reset is destructive and does not preserve these guarantees.

---

# 9. Transactional Outbox

The API must not perform:

```text
DB INSERT
    ↓
RabbitMQ publish
```

without handling the failure between these operations.

Otherwise:

```text
DB INSERT succeeds
       ↓
RabbitMQ publish fails
       ↓
Job exists but is never queued
```

Instead:

```text
BEGIN TRANSACTION

Create job
Create payload
Create idempotency record
Create outbox record

COMMIT
```

After the transaction commits, the outbox publisher publishes the job to RabbitMQ.

If publishing fails, the outbox remains unpublished and can be retried.

If the publisher crashes after publishing but before marking the outbox row as published, the job may be published twice.

This is acceptable because the system uses **at-least-once delivery** and workers must handle duplicate messages safely.

## 9.1 Publication Success Contract

Calling the RabbitMQ client's publish method is not proof that the message reached a queue.

The publisher must:

1. Publish a persistent message to the durable topology described in Section 11.
2. Use publisher confirms and mandatory publishing with returned-message handling to detect routing failures.
3. Mark the specific `outbox_id` as published, and set `published_at`, only after a positive broker confirmation with no routing failure for that publication.

A positive publisher confirmation alone is insufficient: an unroutable mandatory message can be returned and also positively confirmed.

On a negative confirmation, a returned message, a connection failure, or an unknown confirmation outcome, the outbox row remains unpublished and eligible for retry.

If RabbitMQ accepts the message but the publisher crashes before recording success in PostgreSQL, the event is published again. This possible duplicate is preferable to silently losing the job notification.

Publisher confirms are distinct from worker acknowledgements: the former concern broker acceptance; the latter concern consumer processing.

---

# 10. Idempotency

There are two separate idempotency problems.

## Client idempotency

Protects against:

```text
Client
  ↓ POST /jobs
  ↓ timeout
  ↓ retries same request
```

The idempotency key ensures only one job is created.

For a batch, the key protects the whole request: all accepted jobs and rejected-item outcomes are remembered together as described in Sections 5.2 and 8.5. A replay returns the original acceptance response, not the jobs' latest execution statuses; use job retrieval for current state.

## Worker/message idempotency

Protects against duplicate queue delivery.

Example:

```text
Worker A receives J1
Worker A completes J1
Worker A crashes before ACK
RabbitMQ redelivers J1
Worker B receives J1
```

Worker B must recognize that J1 has already completed and must not execute the job again.

These are separate concerns and must not be conflated.

---

# 11. RabbitMQ

V1 uses RabbitMQ as the job execution queue.

Conceptually:

```text
Outbox Publisher
      │
      ▼
Job Exchange
      │
      ▼
job_queue
      │
      ├──── Worker 1
      ├──── Worker 2
      └──── Worker N
```

The queue provides:

- buffering
- producer/consumer decoupling
- competing consumers
- acknowledgements
- redelivery after consumer failure

Message format:

```json
{
  "jobId": "<uuid>"
}
```

The payload is retrieved from PostgreSQL by the worker.

## 11.1 Publication Durability and Routing

- Declare the job exchange and job queue as durable, with the required binding.
- Publish job messages as persistent.
- Enable publisher confirms and mandatory publishing with returned-message handling, following Section 9.1.
- Durable topology and persistent messages support normal broker restarts with storage intact. They do not by themselves guarantee recovery from permanent storage loss.

Section 1.1 defines the V1 failure scope: recover from crashes/restarts and temporary outages with saved data intact. Permanent broker/database storage loss and redundant-node high availability are not covered.

---

# 12. Worker Processing

Worker flow:

```text
Receive message
      ↓
Atomically claim job
      ↓
Create attempt
      ↓
Fetch payload
      ↓
Execute handler
      ↓
Persist result/status
      ↓
ACK message
```

The worker must ACK only after successful database processing.

Important rule:

```text
DB COMMIT
    ↓
RabbitMQ ACK
```

Never ACK first.

## 12.1 Acknowledge After Recording the Execution Outcome

An ACK means this delivery has been handled, not necessarily that the job succeeded. An owned execution outcome is handled only after its complete database transition commits successfully.

| Execution outcome | Database changes before ACK |
| --- | --- |
| Success | Insert result; mark attempt and job `COMPLETED`; record attempt end and job completion times; clear lease |
| Retryable failure with cycle budget remaining | Mark attempt `FAILED` with error details and end time; move job to `RETRYING`; set `next_retry_at`; keep job completion time null; clear lease |
| Permanent failure or exhausted cycle budget | Mark attempt and job `FAILED` with the corresponding error details; record attempt end and job completion times; clear lease |

For each outcome, lock the job row and check current attempt ownership and an unexpired lease, following Sections 14.1 and 20.1. Commit all corresponding changes together, then ACK the delivery. A failed or uncertain database commit is not evidence of a recorded outcome: do not ACK based on an assumption.

For example, inside one short transaction, after locking the job row, accept a retryable first-attempt failure with:

```sql
UPDATE jobs
SET status = 'RETRYING',
      next_retry_at = clock_timestamp() + INTERVAL '1 second',
      lease_expiry = NULL
WHERE job_id = :job_id
   AND status = 'RUNNING'
   AND attempt_id = :attempt_id
   AND lease_expiry > clock_timestamp()
   AND cycle_attempt_count = 1;
```

If one row is updated, mark that attempt `FAILED` with its failure category, reason, and end time in the same transaction. Commit, then ACK. If the attempt update fails, roll back the job update too. If the conditional job update affects zero rows, roll back; this owned-outcome ACK rule does not apply to that path.

This example applies only to a retryable failure on the first claimed attempt of the current cycle. Other attempts use Section 18.1's budget and backoff rules.

After a recorded retryable failure, ACK the old delivery rather than requeueing it to implement backoff. The retry scheduler later creates a new outbox event and notification when the retry is due. The recorded database retry state, not broker redelivery, owns the retry schedule.

Confirmed duplicate/stale delivery follows Section 12.2, database-error recovery follows Section 12.3, and invalid-message handling follows Section 12.4.

## 12.2 Confirmed Duplicate or Stale Deliveries

ACK a confirmed duplicate or stale delivery without executing the handler or writing an outcome as an old owner. Confirm this from authoritative PostgreSQL state, not from a local timeout or a failed database call.

Examples:

- A worker commits completion but crashes before ACK. On redelivery, another worker sees the job is already `COMPLETED`, does not run it again, and ACKs.
- An old worker with attempt A1 resumes after recovery and a later claim have made A2 current. Its conditional completion is rejected. Once the database confirms the ownership change, discard A1's local result and ACK its old delivery; do not change A2 or schedule an additional retry.
- A known job is `RETRYING`: the recorded retry state and scheduler own future progress. The delivery must not bypass the retry time.
- The database confirms the same attempt's lease has expired: the old worker cannot renew or finalize it. Recovery owns recording expiry and deciding the next job state; ACK does not authorize the old worker to make those changes.

A worker can inspect the known job with:

```sql
SELECT status, attempt_id, lease_expiry
FROM jobs
WHERE job_id = :job_id;
```

This read does not grant ownership. New execution still requires the atomic claim; saving an outcome still requires the locked checks described elsewhere.

If a newly received notification finds the job `QUEUED`, try the normal atomic claim. Merely finding that a job already exists does not establish that the delivery should be discarded.

If PostgreSQL is unavailable or the outcome is unclear, do not ACK on the assumption that the delivery is stale. A missing job is not the same as a confirmed completed or superseded attempt; follow Section 12.4 for confirmed missing-job references.

## 12.3 Database Failures and Unknown Commit Outcomes

A connection failure while committing does not prove that the transaction rolled back. PostgreSQL may have committed the outcome before its confirmation was lost.

Do not blindly ACK or rerun the handler. Reconnect and inspect authoritative database state for the original job and attempt:

```sql
SELECT status, ended_at
FROM job_attempts
WHERE job_id = :job_id
   AND attempt_id = :attempt_id;

SELECT status, attempt_id, lease_expiry
FROM jobs
WHERE job_id = :job_id;
```

| Confirmed state | Action |
| --- | --- |
| The original outcome transaction committed | ACK the original delivery; do not execute the handler again |
| The same attempt is still running with a valid lease | Retry saving the already calculated outcome, using a new short transaction with the normal locked ownership/deadline checks |
| The attempt expired or ownership moved elsewhere | Discard the old local outcome and follow Section 12.2; recovery/scheduling owns progress |
| Database unavailable or state still unclear | Keep the outcome unresolved; do not ACK based on a guess |

These reads are for reconciliation, not permission to write. An in-flight transaction may not yet be resolved; every retried write must lock and check current state again. A missing record alone is not proof that a commit failed. Unknown claim outcomes likewise must be resolved before executing a handler as that attempt.

During a database outage, pause taking new work and retry database access with increasing delays capped at a configurable maximum. Do not create an immediate receive/error/requeue loop. This infrastructure backoff is separate from the job's automatic retry budget and 1/2/4-second failure backoff.

Leave unresolved deliveries unacknowledged. If the consumer disconnects, RabbitMQ can redeliver them; resumed processing must still follow database claim and ownership rules. Delivery tags belong to their original channel: do not ACK an old tag on a replacement channel.

Retrying persistence does not authorize another handler invocation or external side effect. A later platform-managed attempt may execute again under the existing at-least-once contract, so business idempotency remains necessary.

## 12.4 Malformed Messages, Missing Jobs, and Unsupported Types

V1 does not add a dead-letter queue. Distinguish an unusable notification from a known job whose failure needs to be recorded.

### Malformed notifications

For invalid JSON, a missing `jobId`, or an invalid job UUID, record a structured error and increment an error metric, then reject the delivery without requeue:

```java
channel.basicReject(deliveryTag, false);
```

`false` means do not return the delivery to the queue. Without a configured dead-letter destination, RabbitMQ discards it. This is an explicit V1 trade-off: logs and metrics expose the problem, but are not a durable message archive or an automatic recovery mechanism. Do not log secrets or entire untrusted payloads.

### Confirmed missing-job references

For a valid UUID, check whether the referenced job exists in authoritative PostgreSQL state:

```sql
SELECT job_id
FROM jobs
WHERE job_id = :job_id;
```

A successful read finding no job means the notification refers to a missing job. Log the reference, increment the error metric, and reject without requeue. Publishing follows job commit, so this is not a normal wait-for-job-creation case. Do not query a lagging replica or confuse a database error or unresolved transaction with proof of absence; follow Section 12.3 for database uncertainty.

This existence read does not claim a job or grant execution ownership. Never create a replacement job solely to satisfy a missing reference.

### Permanently unsupported job types

The API should reject unsupported job types before accepting jobs. V1 workers are expected to support the same platform-approved handler set.

If a known queued job nevertheless has a genuinely unsupported type, claim it normally and create an attempt. Without calling a handler, record `UNSUPPORTED_JOB_TYPE` as a permanent attempt failure, mark the job `FAILED`, and clear its lease in one ownership-checked transaction. Commit, then ACK. If the database operation fails or ownership is lost, follow the corresponding rules above instead of acknowledging an unrecorded failure.

Do not discard that notification and leave known work stuck `QUEUED`. Do not retry a permanently unsupported type indefinitely. A worker missing an approved handler because it runs an incompatible version is a deployment fault, not proof that the job is invalid; stop that worker from consuming incompatible work and expose the fault rather than incorrectly failing valid jobs.

---

# 13. Atomic Job Claim

Multiple workers may receive duplicate messages for the same job.

Workers must atomically claim the job.

Conceptually:

```sql
UPDATE jobs
SET
    status = 'RUNNING',
    attempt_count = attempt_count + 1,
   cycle_attempt_count = cycle_attempt_count + 1,
    attempt_id = :attempt_id,
    started_at = NOW(),
    lease_expiry = :lease_expiry
WHERE job_id = :job_id
  AND status = 'QUEUED';
```

If one row is updated:

```text
Worker owns the job
```

If zero rows are updated:

```text
No execution claim was obtained.
Do not execute the handler.
For a known job confirmed as duplicate/stale, ACK under Section 12.2.
```

Database failures and unknown/missing jobs must not be mistaken for confirmed duplicates. If a fresh read finds the job queued, claim it atomically rather than treating that read as permission to execute.

This operation should use explicit SQL rather than relying purely on ORM-generated queries.

The claim, both attempt counter increments, current attempt ID, lease, and new attempt record must commit in one short database transaction. Use the updated lifetime `attempt_count` as the new attempt's `attempt_number` and the job's newly assigned start time as the attempt's `started_at`, following Section 8.1.1. If creating the attempt fails, roll back the claim and both counters as well.

Execute the job handler only after that transaction commits. Do not keep a database transaction or row lock open while the handler runs.

---

# 14. Worker Lease

A worker temporarily owns a running job through a lease.

Example configuration:

```text
Lease duration:       30 seconds
Heartbeat interval:   10 seconds
Recovery interval:    10 seconds
```

The worker periodically extends the lease.

Conceptually:

```text
Worker
  │
  ├── claim job
  │
  ├── heartbeat → extend lease
  │
  ├── heartbeat → extend lease
  │
  └── complete
```

If the worker crashes:

```text
No heartbeat
    ↓
Lease expires
    ↓
Recovery Manager detects it
```

A fixed timeout alone is insufficient because legitimate jobs may run longer than the timeout.

## 14.1 Permission Ends at Lease Expiry

An expired lease immediately removes the worker's permission to complete or report failure for that attempt, even if recovery has not run yet. An expired lease cannot be renewed by a late heartbeat.

Ownership-changing checks use PostgreSQL's current time after the job row is locked, not the worker's clock. PostgreSQL's `NOW()` is fixed at the transaction start, so use `clock_timestamp()` for these checks to avoid trusting a time from before a lock wait.

The lease must be strictly later than the time checked. Equality counts as expired. This check decides whether to accept a transition while holding the row lock; it is not a guarantee that the transaction will commit before the lease deadline. Keep these transactions short.

Lease timestamps must also be calculated using database time, rather than worker-supplied wall-clock timestamps.

## 14.2 Maximum Runtime per Attempt

V1's initial maximum runtime is **five minutes per attempt**, configurable by the platform. This is separate from the renewable 30-second lease. Heartbeats may keep a healthy attempt alive beyond its initial lease, but must not intentionally keep a timed-out attempt alive indefinitely.

Measure elapsed runtime from that attempt's `job_attempts.started_at`, not from job creation, the first-ever execution, or message receipt. Each new attempt gets its own runtime allowance; the four-attempt cycle budget remains unchanged.

For example, this query detects a running attempt that has reached the initial limit:

```sql
SELECT attempt_id
FROM job_attempts
WHERE attempt_id = :attempt_id
   AND status = 'RUNNING'
   AND started_at + INTERVAL '5 minutes' <= clock_timestamp();
```

Use the configured duration instead of a hard-coded interval in the implementation. This read only detects a possible timeout; it does not grant permission to change the attempt or job.

When the runtime limit is reached:

1. Request cooperative cancellation of the handler and stop renewing that attempt's lease.
2. In a short job-row-locked transaction, recheck current attempt ownership and an unexpired lease before accepting a `TIMEOUT` outcome.
3. If still authorized, record the attempt as `FAILED` with `failure_type = 'TIMEOUT'`. Treat timeout as retryable: schedule retry if cycle budget remains, otherwise mark the job `FAILED`. Commit the full outcome, then ACK under Section 12.1.
4. If ownership is lost or the lease has expired, do not write as the old owner. Recovery owns expiry and the next job state. Follow Sections 12.2–12.3 for delivery and database uncertainty handling.

Cooperative cancellation is not a safe forced thread kill. A blocked or uncooperative handler may continue after cancellation is requested; do not count that request as proof that it has stopped. Configure timeouts on external calls as well. Runtime detection is not a hard guarantee that the process or every external action stops at exactly five minutes; ownership checks and business idempotency remain necessary.

---

# 15. Attempt ID and Stale Workers

`attempt_id` acts as an execution ownership token.

Example:

```text
Worker A → Attempt A1
```

A1's lease expires.

Recovery occurs:

```text
Worker B → Attempt A2
```

Worker A may later resume.

Worker A must not be allowed to update the job using stale attempt A1.

Completion therefore checks:

```text
job_id = J1
AND status = RUNNING
AND attempt_id = A1
AND lease_expiry > current database time checked after locking
```

If A2 is now current, A1's update affects zero rows.

Even if A1 is still the recorded current attempt, an expired lease causes its completion to be rejected. Recovery does not need to run first.

This prevents stale workers from corrupting current job state.

---

# 16. Heartbeat Ownership

A heartbeat should verify that the worker still owns the current attempt.

Conceptually, inside one short transaction (`:lease_duration` is a PostgreSQL interval):

```sql
SELECT job_id
FROM jobs
WHERE job_id = :job_id
FOR UPDATE;

UPDATE jobs
SET lease_expiry = clock_timestamp() + :lease_duration
WHERE job_id = :job_id
  AND status = 'RUNNING'
  AND attempt_id = :attempt_id
   AND lease_expiry > clock_timestamp();
```

If zero rows are updated, the worker has lost ownership and should stop processing safely.

Commit a successful renewal promptly and release the lock. A late heartbeat cannot bring an expired lease back to life.

---

# 17. Recovery Manager

The Recovery Manager periodically searches for running jobs whose leases have expired.

Conceptually:

```sql
WHERE status = 'RUNNING'
AND lease_expiry <= clock_timestamp()
```

For an expired attempt:

```text
Attempt → EXPIRED
Job → RETRYING
next_retry_at → calculated retry time
```

If retry limit has been reached:

```text
Attempt → EXPIRED
Job → FAILED
```

Check the current cycle's `cycle_attempt_count`, not the lifetime counter, when deciding whether to retry. Expired claimed attempts consume the same four-attempt budget as handler failures.

Record the expired attempt's end time with its status change. When recovery makes the job terminally `FAILED`, set its `completed_at`; leave it null when scheduling a retry, following Section 8.1.1.

Recovery must update the relevant database state transactionally.

Before making these changes, recovery must lock the job row and recheck that it is still running with the same expired attempt. An earlier scan only identifies possible candidates; its answer may be out of date by the time recovery gets the lock.

The locked recheck uses fresh database time and the same `<=` expiry boundary. A heartbeat might have extended the lease since the scan; in that case recovery must skip the job.

Update the attempt and job together, then commit and release the lock. Completion and failure handling must use the same job-row locking rule described in Section 20.1.

The Recovery Manager does not need to know which worker crashed. The expired lease is sufficient evidence that the current execution is no longer considered alive.

---

# 18. Retry Strategy

Automatic retry policy:

```text
3 retries
4 total attempts including the initial attempt
```

Backoff:

```text
Attempt failure
     ↓
1 second
     ↓
retry

2 seconds
     ↓
retry

4 seconds
     ↓
retry
```

Jitter may be added later.

Retryable examples:

- Network timeout
- Temporary database failure
- External service 503
- Worker crash
- Attempt exceeds the configurable runtime limit (`TIMEOUT`; initially five minutes)

Non-retryable examples:

- Invalid payload
- Invalid job configuration
- Business validation failure
- Other explicitly classified permanent errors

The **platform owns retry policy**.

Workers report execution outcomes; the platform decides whether to retry.

## 18.1 Lifetime History and Cycle Budget

Each accepted manual retry starts a fresh four-attempt budget without deleting or renumbering older attempts. Keep the same job ID and preserve all previous attempt records.

| Point in execution | `attempt_count` | `cycle_attempt_count` |
| --- | --- | --- |
| After four failures in the initial cycle | 4 | 4 |
| Manual retry accepted, before a worker claim | 4 | 0 |
| First worker claim in the new cycle | 5 | 1 |
| Fourth worker claim in the new cycle | 8 | 4 |

After a retryable failure or expired attempt:

- Cycle count 1: schedule a retry after 1 second.
- Cycle count 2: schedule a retry after 2 seconds.
- Cycle count 3: schedule a retry after 4 seconds.
- Cycle count 4: mark the job `FAILED`; do not schedule another automatic attempt.

A permanent failure stops the cycle immediately, even if budget remains. A successful attempt completes the job. Automatic scheduling never resets the cycle counter. Only a new accepted manual retry resets it.

Counters advance on a committed claim, not on message receipt or HTTP acceptance. A committed claim consumes a slot even if the worker crashes before calling the handler; recovery records that attempt as expired. An infrastructure failure before the claim commits does not consume a slot.

---

# 19. Retry Scheduler

A job waiting for retry remains in PostgreSQL:

```text
status = RETRYING
next_retry_at = <future timestamp>
```

The Retry Scheduler finds:

```text
status = RETRYING
AND next_retry_at <= NOW()
```

It transitions the job to `QUEUED` and creates an outbox record in the same transaction.

Before changing a job, lock its row and recheck that it is still `RETRYING` and due. Only the scheduler that performs that transition inserts the corresponding outbox event. This prevents two scheduler instances from independently scheduling the same transition.

```text
RETRYING
   ↓
QUEUED + outbox
   ↓
Outbox Publisher
   ↓
RabbitMQ
```

The outbox is not itself a delayed-job scheduler.

---

# 20. Job Completion

On successful execution, the worker performs the result and state updates transactionally.

Conceptually:

```text
BEGIN

Lock the job row

Check current ownership and unexpired lease while holding the lock

Create job result

Update attempt → COMPLETED

Update job → COMPLETED
Clear lease

COMMIT

ACK RabbitMQ
```

The ownership check is essential.

A stale worker must not be able to write a result after another attempt has taken ownership.

## 20.1 Lock, Check, Save Together

An ownership check can become outdated between reading the job and saving the result. A transaction alone does not stop another transaction from changing a row read by an ordinary `SELECT`.

For completion, failure handling, and recovery:

1. Begin a short transaction and lock the job row, for example with `SELECT ... FOR UPDATE`.
2. Check the current job state and attempt while holding that lock. Do not rely on an earlier unlocked read.
3. If the transition is allowed, save all related changes together. Successful completion includes the result, attempt status, job status, and cleared lease.
4. If ownership is lost, save no result or attempt/job changes for the old owner. Roll back any preliminary changes.
5. Commit or roll back to release the lock.

All competing ownership transitions must follow this rule. The operation that waits for the lock must check the state again after obtaining it.

The conditional claim in Section 13 already locks the job row as part of its update; it does not require a separate unlocked ownership check.

These locks protect only short database changes, never the full handler execution. Lease expiry removes permission as described in Section 14.1.

## 20.2 Completion Deadline Check

Inside one short transaction, lock first and then conditionally accept completion using current database time:

```sql
SELECT job_id
FROM jobs
WHERE job_id = :job_id
FOR UPDATE;

UPDATE jobs
SET status = 'COMPLETED',
      completed_at = clock_timestamp(),
      lease_expiry = NULL
WHERE job_id = :job_id
   AND status = 'RUNNING'
   AND attempt_id = :attempt_id
   AND lease_expiry > clock_timestamp();
```

- If one row is updated, insert the result and mark the attempt `COMPLETED` in the same transaction, then commit. If either write fails, roll back the completion update too.
- If zero rows are updated, roll back and save no result or attempt/job changes for that worker.

The job update above performs the ownership check and provisional completion before the related writes; none of these changes becomes final until the transaction commits. Worker-reported failure transitions must apply the same ownership and unexpired-lease checks. Recovery, not the expired worker, marks the old attempt `EXPIRED` and decides the next state.

These checks protect database state. They cannot undo an email, payment, or other external action already performed by an expired worker; Section 21's business idempotency remains necessary.

---

# 21. External Side Effects

At-least-once execution means a job may execute more than once.

For operations with external side effects, such as:

```text
PROCESS_PAYMENT
SEND_EMAIL
CREATE_EXTERNAL_ORDER
```

the business operation must have its own idempotency mechanism.

Example:

```text
job_id = J1
payment_id = J1
```

If two workers attempt the payment, the downstream payment system should recognize the same business idempotency key and avoid performing the payment twice.

`attempt_id` protects platform ownership.

A business idempotency key protects the external side effect.

---

# 22. Failure Scenarios

The system should explicitly handle and eventually test:

### API crashes before DB commit

```text
No job created.
Client can retry using same idempotency key.
```

### API crashes after DB commit

```text
Job + outbox exist.
Outbox publisher eventually publishes it.
```

### RabbitMQ publish fails

```text
Outbox remains unpublished.
Publisher retries.
```

### Publisher crashes after RabbitMQ publish

```text
Duplicate message possible.
Worker idempotency handles it.
```

### Worker crashes before claim

```text
Message is redelivered.
```

### Worker crashes after claim

```text
Lease expires.
Recovery Manager handles the job.
```

### Worker crashes after DB completion but before ACK

```text
RabbitMQ redelivers.
New worker sees COMPLETED.
No duplicate execution.
```

### Stale worker resumes

```text
Attempt ID no longer matches.
Stale worker cannot update current job state.
```

---

# 23. Non-Functional Requirements

## Target Status

The performance numbers below are learning targets, not blanket capacity or service guarantees. Bounded local benchmarks and scaling experiments have since been run; their workloads, resources, measured results and invalid attempts are recorded in [the baseline report](benchmarks/2026-10-25.md) and [the scaling report](benchmarks/2026-10-25-scaling.md). They do not demonstrate 1,000/s sustainable capacity or 99.9% availability. Section 1.1 defines V1's intact-storage failure scope and availability limits.

## Throughput

Target peak submission rate:

```text
1,000 job submissions / second
```

## API latency

Job submission:

```text
p99 < 200 ms
```

Measure client-observed HTTP submission latency through receiving the acceptance response, which is sent only after the job, payload, idempotency outcome, and outbox event commit. This does not wait for RabbitMQ publication, worker execution, or job completion.

## Availability

Target:

```text
99.9%
```

This is a longer-term target, not an availability guarantee of V1's single-machine deployment. The platform is unavailable while its host or required dependencies are down; see Section 1.1.

## Durability

Once the API returns `202 Accepted`, the job and required acceptance records are committed. Recover and eventually process accepted work within Section 1.1's failure scope, assuming durable storage remains intact, required services return, and sufficient processing capacity is available. Processing follows the defined success, retry, and terminal failure rules; eventual successful execution is not guaranteed.

This contract does not promise restoration after permanent data loss, corruption, or deliberate volume deletion.

## Execution semantics

```text
At-least-once
```

Exactly-once execution is not guaranteed.

## Processing latency

Under normal peak load:

```text
95% of jobs should begin processing within ~5 seconds
```

This is acceptance-to-first-attempt start delay, including outbox polling/publishing and queue/worker waiting. Acceptance here means the durable acceptance commit, not the later instant the client receives `202`; a worker can start before that response reaches the client. Retry waiting and later attempt starts are measured separately.

## Scalability

Workers must scale horizontally.

## Observability

The system should expose enough information to identify:

- Queue depth
- Job status distribution
- Failure rate
- Retry count
- Queue latency
- Execution latency
- Worker health
- Worker failures
- Database bottlenecks

## 23.1 Measurement and Benchmark Contract

Measure these independently:

| Measurement | Definition |
| --- | --- |
| Submission latency | Client-observed request/acceptance response time, with acceptance after database commit |
| First-start delay | Durable acceptance commit to first attempt start; includes outbox and execution-notification waiting |
| Attempt execution duration | Duration of an individual attempt; report handler-only timing separately if collected |
| Submission throughput | Newly accepted jobs per second, separate from HTTP request rate and idempotent replays |
| Completion throughput | Successfully completed jobs per second; also report terminal failures, attempts, and retries separately |

Use consistent timing boundaries and suitable instrumentation. Do not use the mutable `jobs.started_at` as first-ever start time after retries; the first attempt has its own historical timestamp. If commit timing is approximated in a benchmark, label the approximation rather than presenting transaction-start `created_at` as an exact commit timestamp. Keep client monotonic-duration measurements separate from cross-service clock comparisons.

Initial baseline workloads:

- `ECHO` with a fixed, documented payload/result size to measure the basic platform path.
- `SIMULATED_WORK` with `durationMillis = 1000` and `failFirstAttempts = 0` to measure a known one-second simulated workload without planned failures.

For each run, record workload/type mix, payload and result sizes, expected handler duration, submission rate/concurrency, success/failure settings, run duration and warm-up, hardware/container limits, dependency versions, worker count/capacity, and relevant polling/prefetch settings. State the database's initial data size because retention affects later runs. Report actual errors and the change in outbox/queue/backlog depth, not only successful latency samples.

Measure retry, crash, duplicate-delivery, timeout, and dependency-outage behavior in separate labelled scenarios; do not hide these costs inside an unexplained average or confuse failure-test results with the no-failure baseline.

Accepting jobs faster than they finish grows the backlog. A submission-rate result alone does not establish sustainable processing capacity or the first-start latency target. As a rough capacity check, required active concurrency is arrival rate multiplied by mean attempt service time, before headroom and retry amplification: 1,000 jobs/second with one-second attempts needs approximately 1,000 active slots, not one ten-slot worker. Load tests determine the actual bottleneck and achievable capacity.

---

# 24. Worker Concurrency

Each worker process will use a configurable thread pool.

Initial configuration:

```text
10 concurrent jobs per worker
```

This is a starting point, not a final capacity assumption.

Capacity will be determined through load testing.

Example:

```text
1 worker × 10 threads
5 workers × 10 threads
20 workers × 10 threads
```

The goal is to measure where the bottleneck moves as workers scale.

## 24.1 Bound Intake to Execution Capacity

Initially allow at most ten outstanding unacknowledged deliveries across each worker process, matching its ten configurable handler slots. Keep local waiting work bounded; do not pull a large backlog from RabbitMQ into a worker that cannot start it promptly.

For one consumer feeding ten handler slots, the RabbitMQ Java client can set its per-consumer prefetch before consumption starts:

```java
channel.basicQos(10);
```

Prefetch limits unacknowledged deliveries, not threads or handler executions. With multiple consumers, configure their combined delivery allowance so the worker-wide limit remains ten initially; setting prefetch ten on each consumer would exceed that limit.

Enforce actual handler capacity as well as broker prefetch. A cancellation request does not free a slot until that handler really exits. If a timed-out handler remains alive after its delivery is acknowledged, do not admit replacement execution into its occupied slot or build an unbounded local queue. Reduce or pause intake when no execution capacity is available.

Adjust worker-wide intake together with configured execution capacity and verify the resulting bounds under load and failure tests. Do not treat a prefetch setting alone as a guarantee of bounded execution or prompt starts.

## 24.2 Independent Heartbeat and Timeout Resources

Run heartbeat scheduling and runtime-timeout checks separately from the job-handler pool. Ten busy or blocked handler threads must not prevent lease renewal or timeout detection from being scheduled.

Ensure database access needed by heartbeats is not exhausted by handler activity. Use bounded database usage and sufficient or reserved connection capacity; the exact pool sizes are implementation details to validate against the ten-second heartbeat interval and thirty-second lease.

Handlers must not hold database transactions or connections for their entire execution, especially while waiting for external services. Keep heartbeat database work short and bounded too, so a blocked heartbeat request cannot indefinitely starve other heartbeat or timeout checks.

These safeguards reduce resource starvation; they cannot guarantee heartbeat success during a database outage or a whole-process pause. Existing lease-expiry and recovery rules still apply. Section 24.3 defines the separate broker acknowledgement timeout.

## 24.3 RabbitMQ Acknowledgement Timeout

Use an initial configurable **ten-minute consumer acknowledgement timeout**, giving margin beyond the five-minute per-attempt runtime limit. The RabbitMQ broker setting, in milliseconds, is:

```properties
consumer_timeout = 600000
```

This is a broker setting, not the database lease duration or the application's handler timeout. Verify support and effective configuration against the RabbitMQ version and queue type selected during implementation.

The acknowledgement timer starts at delivery, not handler start. Its allowance must include any local waiting, handler execution, and outcome persistence/ACK time. Database heartbeat renewals do not extend it. Enforcement is periodic rather than an exact ten-minute stopwatch.

If a delivery exceeds the broker's timeout, RabbitMQ can close its consumer channel and requeue that channel's unacknowledged deliveries. A prolonged database outage can still cause this despite bounded worker intake. Follow Sections 12.2–12.3 on redelivery and outcome reconciliation; never ACK an old channel's delivery tag on a replacement channel. Broker redelivery does not by itself revoke database attempt ownership.

Review this timeout whenever attempt runtime, prefetch/local waiting, or persistence expectations change. It is an initial operating setting, not a guarantee that all execution and database recovery finish within ten minutes.

---

# 25. Implementation Strategy

Implementation should happen incrementally.

## Phase 1 — Foundation

- Spring Boot project
- Maven
- PostgreSQL
- Docker Compose
- JPA
- Flyway
- Basic configuration
- Health endpoint

## Phase 2 — Job Persistence

- Database migrations
- Entities
- Repositories
- Job creation
- Job retrieval

## Phase 3 — Job API

- `POST /jobs`
- `GET /jobs/{jobId}`
- Validation
- Error handling
- Idempotency

Acceptance already commits the required outbox row with the job and idempotency outcome. The schema must therefore support this path even though the publisher is introduced in Phase 4.

## Phase 4 — Messaging

- RabbitMQ
- Outbox
- Outbox Publisher
- Queue
- Worker
- ACK handling
- Atomic claim and attempt creation in one transaction
- Initial lease and attempt ownership checks on outcome writes
- Transactional result/attempt/job outcome persistence before ACK
- Duplicate-message protection and tests for claim races/redelivery

These are minimum safety requirements when queue execution first appears, not work to defer until Phase 5. Introduce the minimal correct processing path together: no handler execution without a committed claim/attempt, no stale-owner outcome writes, and no ACK before recording an owned outcome. Respect the agreed intact-storage durability and publisher-confirm rules from the start.

Phase 4 alone is not the completed reliable V1: without later renewal/recovery, a crash after claim can still leave work waiting for a recovery mechanism. Do not advertise full crash recovery or retry behavior before Phase 5 is implemented and tested.

Historical Phase 4 scope was ECHO-only execution, with simulations deferred and no renewal/recovery. Phase 5 has since enabled both handlers and their reliability controls, described in [messaging.md](messaging.md) and [reliability.md](reliability.md). Mandatory confirmed publishing, explicit SQL ownership transitions, and commit-before-ACK remain unchanged; ten synchronous consumers each have prefetch one.

## Phase 5 — Reliability

- Extend the attempt tracking and ownership baseline introduced in Phase 4
- Retry policy
- Retry Scheduler
- Extend the initial worker lease with renewal and recovery
- Heartbeat
- Recovery Manager
- Stale-worker protection

Automatic retry scheduling, heartbeat renewal, timeout/recovery behavior, and their failure tests extend Phase 4's baseline. They must preserve its atomicity, ownership, and commit-before-ACK rules rather than adding safety only after unsafe queue processing has shipped.

Implemented Phase 5 uses independent four-thread supervision and a separate four-connection control pool for bounded heartbeat/runtime checks. Retry/recovery candidate scans remain bounded; each job transition rechecks database time and state under its row lock. Manual retry uses the existing scoped request-outcome transaction for replay-before-eligibility and atomic cycle/outbox reset. Both handlers execute; simulation uses current-cycle position. Success persistence rechecks runtime as well as lease ownership. See [reliability.md](reliability.md) for exact defaults, cancellation limits, and the actual killed-worker test scope. No migration beyond V2 was needed.

## Phase 6 — Observability

- Metrics
- Structured logging
- Health checks
- Prometheus
- Grafana

Implemented Phase 6 adds best-effort after-commit lifecycle counters/timers and ECS JSON logs, local worker/pool gauges, periodic cached database snapshots with explicit health/freshness, native RabbitMQ metrics, and optional localhost Prometheus/Grafana services with a provisioned dashboard. Scrapes never query PostgreSQL. Creation-to-first-claim timing is labelled an approximation, not exact commit-to-start delay; server HTTP timings are not client round trips. Metric labels exclude IDs, payloads, keys and raw error details. See [observability.md](observability.md) for units, restart/commit observation gaps, snapshot cost, configuration, dashboard use, and validated limits. Performance and availability targets remain unmeasured.

## Phase 7 — Performance & Failure Testing

- Load testing
- Throughput measurements
- Latency measurements
- Worker crash testing
- Duplicate message testing
- Database failure testing
- RabbitMQ failure testing
- Bottleneck analysis

Implemented Phase 7 uses isolated disposable dependencies, fixed bounded open-loop arrivals, explicit client/first-start/attempt timings, separate observed database completion/drain windows, and retained resource/error/backlog evidence. Fixed ECHO and one-second no-failure simulation baselines plus a separate overload case were repeated. Actual database and broker outages/return now supplement existing duplicate, timeout, pool-starvation and killed-worker tests. See [performance.md](performance.md) and [the measured report](benchmarks/2026-10-25.md). Ten simulation slots saturated under 15 offered jobs/s despite fast API acceptance; one-minute local measurements do not establish 1,000/s capacity, exact commit-to-start SLO, long-term availability, or permanent-loss resilience. Subsequent local Phase 8 results are recorded below.

## Phase 8 — Scaling

- Dockerized deployment
- Kubernetes
- Horizontal worker scaling
- Resource tuning

**Implemented local scope:** role-separated API/publisher and one/two worker process experiments, equal-slot control, a scaled overload and experimental publication-poll tuning, plus a digest-pinned non-root container image and independent scalable Compose deployment verified in a disposable project. Connection/resource costs and invalid attempts are reported, not hidden. See [scaling.md](scaling.md) and [the scaling measurements](benchmarks/2026-10-25-scaling.md). No production execution defaults/schema changed. **Kubernetes, multi-host deployment/HA and autoscaling remain deferred**; local milestone completion does not claim these original exploration items exist or that every numerical target was met.

---

# 26. Engineering Principles

The implementation should follow these principles:

1. Prefer simple designs before adding complexity.
2. Do not add infrastructure solely for resume keywords.
3. Make concurrency behavior explicit.
4. Prefer database constraints for data invariants.
5. Use transactions where state transitions must be atomic.
6. Use explicit SQL for concurrency-critical operations.
7. Assume messages can be duplicated.
8. Assume workers can crash at any point.
9. Treat external side effects as potentially non-idempotent.
10. Measure performance instead of assuming scalability.
11. Every major distributed-systems feature should have a failure scenario/test.
12. Avoid implementing future phases prematurely.

---

# 27. Agent Development Rules

AI coding agents should treat this document as the architectural source of truth.

When implementing a GitHub issue:

- Implement only the requested issue.
- Follow the architecture defined here.
- Do not introduce new infrastructure without justification.
- Do not replace PostgreSQL, RabbitMQ, JPA, or Flyway with alternatives.
- Do not redesign existing architecture without an explicit design decision.
- Add tests for new behavior.
- Keep changes focused.
- Update documentation when behavior changes.
- Prefer small, reviewable commits/PRs.
- Do not implement future milestones unless explicitly requested.

The human developer owns architectural decisions. AI agents assist with implementation.

---

# 28. Future Exploration

Possible future experiments:

- Kafka-based implementation
- PostgreSQL read replicas
- Database partitioning
- Redis-based scheduling/caching
- Advanced backpressure
- Dead-letter queues
- Priority queues
- Job cancellation
- Scheduled jobs
- DAG/workflow execution
- Multi-region architecture
- Kubernetes autoscaling
- Advanced distributed ID generation

These are deliberately outside the initial implementation scope.