# Job API — Phase 3

Base URL: http://localhost:8080. Start the platform using [the README](../README.md).

The API accepts/stores jobs independently of execution. With messaging enabled,
both `ECHO` and `SIMULATED_WORK` now execute through RabbitMQ with Phase 5 retry,
heartbeat, timeout, and recovery controls. Plain startup defaults to API-only
mode, so execution/polling then waits. See [messaging.md](messaging.md) and
[reliability.md](reliability.md) for execution semantics and limits.

## Submit one job

```http
POST /jobs
Content-Type: application/json
Idempotency-Key: demo-echo-1

{"type":"ECHO","payload":{"message":"hello"}}
```

After the database transaction commits, return HTTP **202 Accepted** with a
`Location: /jobs/<uuid>` header and:

```json
{"jobId":"<uuid>","status":"QUEUED"}
```

Allowed types: `ECHO` and `SIMULATED_WORK` (case-sensitive). Only `type` and
`payload` are allowed at the top level. `ECHO` accepts any non-null JSON payload,
including an empty object. Simulation payloads contain exactly `durationMillis`
and `failFirstAttempts`, both nonnegative whole numeric values. Duration fits a
signed 64-bit integer; failure count fits a signed 32-bit integer. Equivalent
whole-number spellings such as `1`, `1.0`, and `1e0` are accepted consistently.
Durations exceeding five minutes are allowed so later timeout tests can run;
they do not bypass the configurable five-minute per-attempt runtime limit.
Timeout is retryable within the remaining four-attempt cycle budget. Simulation
success returns duration/cycle-position data, not a real external service result.

## Idempotency and transaction behavior

- Supply exactly one nonblank `Idempotency-Key` header of at most 200 characters.
- The server assigns client identity `local-client`; no authentication or tenant
  isolation is provided by this namespace. All callers share it.
- Single requests use scope `SUBMIT_JOB`; batches use `SUBMIT_BATCH`. A matching
  key in another scope is a separate request.
- Same scoped key and same input: return the saved HTTP status/body, including
  the original job IDs, even if execution state later changes.
- Same scoped key but different input: HTTP **409 Conflict** with error code
  `IDEMPOTENCY_KEY_REUSED`. Use a new key for genuinely new work.
- Compare a `v1:`-prefixed SHA-256 fingerprint of normalized parsed JSON. Ignore
  formatting and recursively sort object fields. Preserve string contents,
  types, absent-versus-null distinctions, and array/batch order. Numerically
  equivalent JSON numbers match (`1` and `1.0`); strings such as `"1"` do not.
  Arbitrary-precision parsing avoids rounding large integers/decimal input.
- Duplicate object fields, malformed JSON, and trailing JSON values are rejected
  rather than interpreted ambiguously.
- Persist the job, payload, unpublished outbox event, and saved response/fingerprint
  in the **same transaction**. Database uniqueness makes competing same-key
  submissions wait for the original transaction; no in-memory/distributed lock is
  used. A reservation inside that transaction is finalized or rolled back, never
  committed as an incomplete response.
- Item-validation errors for a well-formed single request are stored/replayed as
  400 too. Fixing their input requires a new key. Malformed JSON, a non-object
  envelope, invalid batch structure, or an invalid/missing key is not recorded.
- Database failure returns a sanitized **503**, not acceptance. Retry the same
  input/key after the dependency returns, since a lost commit confirmation can
  leave the outcome uncertain. Never change the key just because of a timeout.
- Keep idempotency records indefinitely for now; no automatic cleanup is enabled.

## Submit a batch

```http
POST /jobs/batch
Content-Type: application/json
Idempotency-Key: demo-batch-1

{
  "items": [
    {"type":"ECHO","payload":{"message":"hello"}},
    {"type":"UNKNOWN","payload":{}},
    {"type":"SIMULATED_WORK","payload":{"durationMillis":1000,"failFirstAttempts":2}}
  ]
}
```

The envelope has only `items`, a nonempty array of at most **100 jobs**. All
items are validated before any job inserts. Invalid individual items are rejected;
valid ones are committed together with their payloads/outbox and the full batch
response. A database error rolls the whole acceptance transaction back.

Return **202** when any item is accepted, otherwise **400**. The response is:

```json
{
  "acceptedCount": 2,
  "rejectedCount": 1,
  "items": [
    {"index":0,"outcome":"ACCEPTED","jobId":"<uuid-1>","status":"QUEUED"},
    {"index":1,"outcome":"REJECTED","error":{"code":"UNSUPPORTED_JOB_TYPE","message":"Supported job types are ECHO and SIMULATED_WORK"}},
    {"index":2,"outcome":"ACCEPTED","jobId":"<uuid-2>","status":"QUEUED"}
  ]
}
```

`index` is zero-based and follows input order. A repeated matching batch returns
the same outcomes and IDs, including rejected items. The fingerprint includes
invalid items too. A properly structured all-invalid batch stores/replays its
400 response without jobs or outbox events. Reordering items changes the request.

## Read one job

`GET /jobs/{jobId}` returns **200** with job ID/type/status, payload, lifetime and
cycle counts, current/latest attempt pointer, lease/retry timestamps, original
creation time, current-cycle latest start, and terminal completion time. Before
execution, the attempt and execution timestamps are null and counters are zero.

`failure` is null for new-cycle, running, queued, and completed work. For current
failed/retrying work it can contain the relevant attempt's `type`/`reason`, not an
unrelated historical failure. `result` is null until completion; when stored it
contains `storageType`, `value`, and `location`. Both job types have real worker
execution and automatic retries/recovery in Phase 5.

An ordered `attempts` array includes attempt ID/number, worker ID, status,
start/end, failure type, and failure reason. Lifetime numbers never reset on
manual retry. History is currently returned in full; pagination for very long
histories remains future work.

An unknown valid UUID returns **404**/`JOB_NOT_FOUND`; an invalid UUID returns
**400**/`INVALID_PARAMETER`. Retrieval uses a consistent read-only database
snapshot and never claims a job or changes its state.

## List jobs

`GET /jobs?status=QUEUED&limit=20` returns:

```json
{"jobs":[{"jobId":"<uuid>","type":"ECHO","status":"QUEUED","attemptCount":0,"createdAt":"<timestamp>"}],"limit":20}
```

Status is optional and must be one of `QUEUED`, `RUNNING`, `RETRYING`, `COMPLETED`,
or `FAILED`. Results are newest-first with job ID as a tie-breaker; payloads are
not loaded for this summary. `limit` defaults to 100 and accepts 1–100. This is a
bounded first-page listing, **not** a full cursor/pagination API; older results
can require later pagination work.

## Manually retry a failed job

```http
POST /jobs/<uuid>/retry
Content-Type: application/json
Idempotency-Key: retry-cycle-1

{}
```

The body may be absent or empty. Existing type/payload are unchanged. A new key
on a `FAILED` job accepts a fresh cycle with **202** and the same
`{"jobId":"<uuid>","status":"QUEUED"}` body/Location. It resets only the cycle
counter and current job start/completion/retry/lease timestamps, preserving
creation and all past attempts; the next worker claim creates the new attempt.

Scope is `RETRY_JOB:<canonical-lowercase-uuid>`. Same scoped key/input replays its
saved response before checking current state; a new key targeting an existing
non-failed job is **409**/`JOB_NOT_FAILED`. Unknown ID is **404**. Nonempty input
is **400** for a new request, or **409** if it changes a previously saved request.
Saved well-formed 400/404/409 retry outcomes replay as well; use a new key for an
intentional new operation after eligibility changes. No automatic key expiry.

Multiple manual cycles are allowed. Reusing an old accepted key after the job
fails again returns its original response and **does not restart it**. See
[reliability.md](reliability.md) for examples and concurrency/budget details.

## Error format

For implemented job routes, errors use:

```json
{"error":{"code":"INVALID_PAYLOAD","message":"payload is required"}}
```

| HTTP status | Meaning |
| --- | --- |
| 400 | Invalid key, JSON, envelope, job input, UUID/status/limit, or all-invalid batch |
| 404 | Unknown job |
| 409 | Scoped key reused with changed input, or a new manual retry targets non-failed work |
| 415 | Unsupported request content type; send `application/json` |
| 503 | Database operation failed/commit uncertain; retry same input/key |
| 500 | Unexpected internal error; no SQL, stack trace, or database details returned |

No request/response byte-order guarantee is made for JSON object fields: JSONB
may order keys differently. Replays preserve the stored status and JSON content.

## Tests

Use **Verify project (Java 21, macOS)** or `mvn verify` under Java 21 with Docker
running. Tests use disposable PostgreSQL and real HTTP servers, including racing
matching/changed requests, batches, precision, replay, and injected database
failures. Failure fixtures never run against the persistent Compose database.