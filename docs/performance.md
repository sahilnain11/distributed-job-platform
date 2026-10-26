# Performance and failure experiments — Phase 7

These are **bounded local learning experiments**, not a production capacity or
availability certification. They keep acceptance, execution, backlog, errors and
recovery separate. Performance targets in the design remain targets unless the
particular measurement and conditions actually support them.

Completed evidence: [25 October results and backlog chart](benchmarks/2026-10-25.md)
and [machine-readable retained summary](benchmarks/2026-10-25-summary.json).

## Repeat the load experiments

Requirements: Java 21, Maven, Python **3.9+**, and running Docker Desktop. No Python
packages are required. From VS Code's **Terminal → Run Task**:

1. **Verify project (Java 21, macOS)** builds the executable JAR and runs all Java
   regression/failure tests.
2. **Verify benchmark runner** runs the Python measurement/safety unit tests.
3. **Run isolated learning benchmarks (Java 21, macOS)** launches one separate
   application with **new disposable PostgreSQL/RabbitMQ containers**, runs the
   workloads below and writes its reports.

The runner [tools/benchmark.py](../tools/benchmark.py) uses random localhost ports,
an empty database and unique request-key prefixes. It does not submit to, stop,
truncate, or delete the existing development platform. The experimental app binds
to loopback. On exit, only the run's exact-label-verified containers and their
disposable volumes are removed. No Compose down/prune is used. Persistent
development job retention is unchanged.

Each invocation prints its report directory under the ignored build output.
It contains raw requests, job/attempt history and sampled backlog in JSON,
application JSON logs, a readable Markdown summary, and a backlog SVG. Keep the
directory if raw evidence is needed: a build-output clean can remove it. Use
Markdown Preview to render the generated chart. Jar and runner SHA-256 hashes,
timestamps, resource limits, initial database size and settings are recorded.

CLI help exposes measured/warm-up/drain durations, bounded request concurrency and
case selection. Limits deliberately prevent an accidentally unbounded run. The
supplied task uses these defaults:

| Case | Workload | Offered jobs/s | Warm-up | Measured submission window |
| --- | --- | ---: | --- | --- |
| ECHO baseline | Same 128-character ASCII message every job, no side effects | 50 | 10 seconds at 10/s, fully drained | 60 seconds |
| Simulation baseline | `durationMillis=1000`, `failFirstAttempts=0` | 5 | 10 seconds at 5/s, fully drained | 60 seconds |
| Overload, separate scenario | Same no-failure one-second simulation | 15 | 10 seconds at 5/s, fully drained | 60 seconds |

The default generator has **16 HTTP threads and at most 32 outstanding requests**.
Every HTTP request uses a fresh local TCP connection, no TLS and one single-job
POST with a unique key. Payload/input is fixed within a case; it is not a batch
throughput test. Warm-up jobs remain in this disposable database, but distinct
key selection excludes them from measured results. Cases share the same
experimental application/database sequentially, so the initial retained data
size is recorded separately for each case. A later case is not an empty-DB run.

## Measurement boundaries

- **Open-loop arrivals:** requests have a fixed arrival schedule, not "send the
  next request after the previous one finishes." Outstanding work is bounded;
  missed/late/deadline arrivals are recorded rather than queued without limit.
  Duplicate CLI cases are rejected so they cannot secretly benchmark replay.
- **Client acceptance latency:** monotonic time immediately before HTTP send to
  receipt/parsing of the complete 202 response. It includes local connection and
  response costs; it excludes generator scheduling/queue delay, which is reported
  separately. Scheduled-arrival-to-response latency is also retained to make
  generator lag visible. Percentiles use nearest rank; no-event values are null.
- **Errors:** HTTP status errors, protocol/network failures and generator drops
  remain in the report, not just successful samples. A network timeout can have
  an **unknown commit outcome**, never a proven rejection. Such runs are explicitly
  not clean measurements and do not proceed silently into another case. The
  benchmark does not retry submissions into the measured workload.
- **Completion rate:** job completions between independent PostgreSQL start/end
  snapshot timestamps, with the initial count subtracted. That database window
  can be slightly longer than the nominal offered-load window because observing
  state has cost. Its actual duration is recorded; drain completions are excluded.
- **Backlog:** exact selected-cohort unfinished stored jobs, unpublished outbox
  rows and running state sampled about every five seconds on a separate observer
  thread. RabbitMQ management ready/unacknowledged counts accompany them, but its
  statistics are delayed and not atomic with the database sample. Do not add
  these different-stage counts together.
- **First-start delay:** database creation to the first attempt's start. This is
  explicitly an **approximation** that includes acceptance precommit time, not
  exact durable-commit-to-start. Later attempts do not replace the first start.
  Never-started jobs count against the within-five-seconds fraction; latency
  distributions necessarily describe only attempts that actually started.
- **Attempt duration:** stored attempt start to end; includes platform/persistence
  overhead. It is not handler CPU time. A one-second simulation should be at
  least approximately one second. No cross-host wall-clock subtraction is used.
- **Drain:** after arrivals stop, wait up to 120 seconds for the cohort's database
  jobs to become terminal and its outbox to be published, checking every two
  seconds. Report the additional observed drain time and unresolved jobs. This
  is database/outbox completion, **not** an exact final-Rabbit-ACK timestamp.
- **Correctness:** inspect every selected job's status, attempts and result.
  Report wrong results, failures, unfinished work and actual payload/result byte
  sizes; compare acknowledged 202 counts with stored job counts. No success-only
  percentiles may hide dropped arrivals or unknown commits.

Observer SQL/HTTP traffic and INFO lifecycle logs consume resources. The runner
does not disable telemetry to inflate the results, and its own scheduling lag is
part of the evidence. Default benchmark heap/CPU/dependency limits are recorded;
do not compare another machine or resource configuration without stating changes.

## Failure experiments

The [dependency-outage integration tests](../src/test/java/com/sahil/jobplatform/messaging/DependencyOutageIntegrationTests.java)
stop and restore the **actual services** inside disposable containers. They do
not inject an exception and call it an outage, nor replace the database with an
empty one. PostgreSQL keeps its container ID, port, URL and cluster identifier.
RabbitMQ keeps its broker data/container. Restoration is attempted in `finally`.

| Scenario | Tested observation |
| --- | --- |
| Broker application stopped | Fresh AMQP connection fails; readiness for API/database acceptance remains healthy; HTTP submission/replay returns 202; job remains QUEUED with no attempt and an unpublished outbox row |
| Broker returns | Consumers reconnect without a manual post-return start; confirmed durable backlog from before the outage and new pending work both complete once; outbox/queue drain |
| PostgreSQL stopped | POST, job GET and readiness return 503, not false acceptance; previously completed work survives; rejected request left neither job nor saved key |
| PostgreSQL returns | Retrying the same key commits one job; replay remains stable and changed input conflicts; original stored data remains |
| PostgreSQL stopped during active simulation | Real prior heartbeat is observed; supervision becomes uncertain, cancels cooperative waiting and pauses intake; actual lease expires while DB is down |
| Recovery after active-attempt outage | History becomes EXPIRED then COMPLETED, one replacement result; old attempt success/failure writes rejected; retry/outbox progresses after DB returns |

These use shortened **test-only** settings: two consumers, two-second lease,
150ms heartbeat, 50ms control tick, 100ms poll/retry intervals, and a 20-second
runtime ceiling. They also shorten connection/query timeouts. Their observed
recovery times are not production-default recovery guarantees.

Existing tests remain part of Phase 7's validation:

- [ReliabilityMessagingIntegrationTests](../src/test/java/com/sahil/jobplatform/messaging/ReliabilityMessagingIntegrationTests.java):
  separate JVM forcibly killed after claim; survivor expires/replaces its attempt;
  actual cooperative watchdog cancellation, retries/exhaustion and manual cycles.
- [RabbitMessagingIntegrationTests](../src/test/java/com/sahil/jobplatform/messaging/RabbitMessagingIntegrationTests.java):
  confirmed publication/DB-mark failure and duplicate resend; one committed
  execution/result; real channel-close redelivery after completion before ACK.
- [ReliabilityStoreTests](../src/test/java/com/sahil/jobplatform/messaging/ReliabilityStoreTests.java):
  independent supervision with every primary connection held, strict stale-owner
  protection, runtime guard and atomic competing recovery/retry transitions.

Timeout/crash tests use short settings or isolated timestamp fixtures rather than
waiting through four five-minute production attempts. This is **not** a full
five-minute timeout soak, arbitrary Java force-stop, network-partition campaign,
permanent-volume-loss recovery, or long-term availability measurement.

## Interpretation rules

Fast acceptance is not proof of fast execution. With ten slots and roughly
one-second attempts, about ten jobs/second is the rough ceiling before overhead.
Offering fifteen jobs/second should increase waiting work even when every HTTP
response is fast. Once input stops, that work may finish successfully; counting
those completions as if they happened during submission would hide overload.

There is another configured bound: one publisher reads at most 100 rows per
scheduled invocation and waits one second **after** the invocation completes.
Its steady publication rate therefore cannot reach 1,000 events/s with these
defaults. This is a configuration-derived bound, not a measured ECHO capacity
limit. More workers alone will not eliminate every bottleneck.

Do not claim 99.9% availability, indefinitely sustainable capacity, or exact
commit-to-start percentiles from a few one-minute runs. Repetition gives a local
comparison, not a confidence interval. Long-duration and larger-retention
experiments remain separate work. Subsequent local multi-worker,
resource/poll tuning and container validation are now recorded in
[the scaling guide](scaling.md) and [measured report](benchmarks/2026-10-25-scaling.md).
Kubernetes and multi-host HA remain deferred.