# Local development

Run commands from the checkout root. This guide covers the persistent local
environment; the [README](../README.md) summarizes features and measured evidence.
The API has **no authentication**. Use a trusted development machine, keep
services private, and never reuse the bundled development passwords in production.

## Prerequisites and Java selection

- JDK **21** and Maven **3.9+**; both `java -version` and `mvn -version` must use 21.
- Running Docker Desktop, or Docker Engine with Compose v2.
- Python **3.9+** for tooling tests and experiment runners; no Python packages
  need installing for these standard-library tools.
- Free default host ports: 5432 (PostgreSQL), 5672/15672 (RabbitMQ), 8080 (app),
  and optionally 9090/3000 (Prometheus/Grafana).

On macOS, select an installed JDK 21 in the project terminal:

```sh
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
export PATH="$JAVA_HOME/bin:$PATH"
java -version
mvn -version
```

This does not edit global shell settings. Maven's enforcer rejects other JDK
major versions. Other operating systems should select Java 21 appropriately.

## Start one host application

```sh
docker compose up -d --wait postgres rabbitmq
SERVER_ADDRESS=127.0.0.1 MESSAGING_ENABLED=true mvn spring-boot:run
```

Keep that terminal running. If the application is already running, reuse it or
stop it before relaunching; **do not start two applications on port 8080**.
See the [README job example](../README.md#quick-start) or [API contract](api.md)
to submit work and check execution.

For API-only work, start just PostgreSQL and use:

```sh
docker compose up -d --wait postgres
SERVER_ADDRESS=127.0.0.1 MESSAGING_ENABLED=false mvn spring-boot:run
```

`MESSAGING_ENABLED=false` is the application default. Acceptance and manual retry
can persist work in this mode, but publishing, consumption, and retry/recovery
pollers do not run. Enable messaging later against the same database to execute it.

Flyway initializes the `job_platform` schema and applies V1/V2 migrations;
Hibernate validates mappings rather than creating tables. Existing volumes
upgrade at startup. Do not edit applied migrations or delete data to force an
upgrade. Required database or migration failure prevents successful startup.

### Health is not proof of worker progress

```sh
curl --fail-with-body http://localhost:8080/actuator/health
curl --fail-with-body http://localhost:8080/actuator/health/readiness
curl --fail-with-body http://localhost:8080/actuator/health/liveness
```

- **Overall health:** PostgreSQL and, when messaging is enabled, RabbitMQ;
  healthy responses contain `"status":"UP"` without connection details.
- **Readiness:** application/database acceptance, **not** queue drainage or worker
  progress. It can remain UP during a broker outage while the outbox stores work.
- **Liveness:** application process state, not dependency health or successful jobs.

Use job GET responses, attempt history, backlog, and worker metrics to verify
execution. A saved `202` response is durable acceptance, not completion.

## Configuration and local credentials

Sources: [application settings](../src/main/resources/application.yml),
[development Compose](../compose.yaml), and [environment example](../.env.example).
The values below are **development placeholders**, not production configuration.

| Variable | Default | Notes |
| --- | --- | --- |
| `DB_URL` | `jdbc:postgresql://localhost:5432/job_platform` | Host application's JDBC URL |
| `DB_USERNAME` / `DB_PASSWORD` | `job_platform` / `local-development-only` | App and development PostgreSQL |
| `DB_PORT` | `5432` | Compose host port; also update `DB_URL` when changing it |
| `DB_POOL_SIZE` | `10` | Primary pool per process; supervision has a separate four-connection pool |
| `SERVER_PORT` | `8080` | Host application HTTP port |
| `SERVER_ADDRESS` | Unset; Spring Boot normally binds all interfaces | Set `127.0.0.1` for local-only host access |
| `MESSAGING_ENABLED` | `false` | Master switch for messaging and reliability pollers |
| `PUBLISHER_ENABLED` / `WORKER_ENABLED` | `true` / `true` | Role switches, effective when messaging is enabled |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `localhost` / `5672` | App broker address; port also controls Compose host binding |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | `job_platform` / `local-rabbit-development-only` | Development broker login |
| `RABBITMQ_MANAGEMENT_PORT` | `15672` | Compose management UI host port |
| `PROMETHEUS_PORT` / `GRAFANA_PORT` | `9090` / `3000` | Optional monitoring ports, bound to loopback |
| `GRAFANA_USERNAME` / `GRAFANA_PASSWORD` | `job_platform` / `local-grafana-development-only` | Initial admin account on a new Grafana volume |

Compose reads a root `.env` file for interpolation. **Maven-launched Spring Boot
does not automatically load it.** If copying the example, export changed values
in the application shell too, or pass Spring configuration explicitly. In
particular, the example's `MESSAGING_ENABLED=true` is not the application's default.

PostgreSQL initialization credentials only apply to a new data directory;
editing environment variables does not alter users/passwords in an existing
volume. Likewise, Grafana's initialization variables do not reset an existing
admin account. Do not delete volumes as a password-change workaround.

### Execution defaults versus experimental tuning

| Spring property / control | Default |
| --- | --- |
| `platform.messaging.worker-concurrency` | 10 synchronous consumer slots; prefetch 1 per consumer |
| `platform.messaging.publisher-delay-ms` / `batch-size` | 1000ms / 100 candidates |
| `platform.messaging.confirm-timeout-ms` | 5000ms |
| `platform.messaging.lease-seconds` | 30 seconds |
| `platform.reliability.heartbeat-ms` / `control-interval-ms` | 10000ms / 1000ms |
| `platform.reliability.max-runtime-ms` | 300000ms (five minutes per attempt) |
| Retry budget / backoff | Four total attempts per cycle; minimum 1/2/4 seconds |
| `platform.reliability.retry-poll-ms` / `recovery-poll-ms` | 1000ms / 10000ms; up to 100 candidates per poll |
| `platform.reliability.pollers-enabled` | `true`, subject to master messaging switch |
| `platform.observability.snapshot-enabled` / `snapshot-interval-ms` | `true` / 10000ms |
| RabbitMQ `consumer_timeout` | 600000ms; independent delivery/channel safeguard |

Use Spring property overrides deliberately. For example, the scaling report's
twenty-slot worker and 100ms publisher delay were **experimental settings**, not
changed application defaults. More processes multiply pools, threads, heap,
supervision, and snapshot queries. Faster polling adds database/broker work.
See [messaging](messaging.md), [reliability](reliability.md), and
[scaling](scaling.md) before tuning. Runtime cancellation cannot force-stop
arbitrary Java code or make downstream effects exactly-once.

## Build, tests, and packaged execution

```sh
mvn verify
python3 -m unittest discover -s tools -p 'test_*.py' -v
```

Java integration tests require Docker and use disposable **real PostgreSQL 17.6
and RabbitMQ 4.1.8** containers. They do not use H2, require existing development
services, or silently skip missing Docker. Tests apply fixtures/faults only to
their own dependencies, including actual outages and a forcibly killed worker
JVM. The existing Java and Python suites remain the verification entry points.

The latest local publication-readiness results are **173 Java / 79 Python tests
passed on 2026-10-26**, including eight publication-guard tests beyond the earlier
71 tooling tests. Earlier benchmark/deployment reports retain their original dates;
see [progress](progress.md) and the [scaling report](benchmarks/2026-10-25-scaling.md).
The [first hosted GitHub Actions verification](https://github.com/sahilnain11/distributed-job-platform/actions/runs/40377713810)
also passed on 2026-10-26, including the real-dependency suite and disposable
container smoke. Later workflow results remain separate from that recorded evidence.

Before publishing, stage only intended project files and run
`python3 tools/check_repository.py`. This focused guard checks tracked paths,
ignore probes, local document links and common secret patterns; it is not a full
credential/history audit. See [contribution rules](../CONTRIBUTING.md).

`mvn verify` produces the executable JAR. To use it instead of the Maven launcher
(stop any existing host app first):

```sh
SERVER_ADDRESS=127.0.0.1 MESSAGING_ENABLED=true \
  java -jar target/distributed-job-platform-0.0.1-SNAPSHOT.jar
```

## Optional monitoring

Keep the host application running separately; these services do not launch it:

```sh
docker compose --profile monitoring up -d --wait postgres rabbitmq prometheus grafana
```

- [Grafana dashboard](http://localhost:3000/d/job-platform-overview): initial user
  **job_platform**, password **local-grafana-development-only**.
- [Prometheus targets](http://localhost:9090/targets): expect application and
  RabbitMQ targets UP before interpreting graphs.
- [Application metrics](http://localhost:8080/actuator/prometheus).
- [RabbitMQ management](http://localhost:15672): user **job_platform**, password
  **local-rabbit-development-only**.

**Host networking caveat:** the configured application scrape uses
`host.docker.internal:8080`, not container-local `localhost`. A host app bound to
`127.0.0.1` may not be reachable through that route, depending on Docker's host
networking. If the app target is DOWN, check reachability before trusting graphs.
Only on a trusted, firewall-restricted development machine, stop the app and
relaunch with `SERVER_ADDRESS=0.0.0.0` if necessary; this exposes the unauthenticated
API/metrics on host interfaces. Do not launch another copy or widen exposure on
a public network. Keep loopback binding when monitoring is unnecessary.

Changing `SERVER_PORT` also requires updating the application target in the
[Prometheus scrape configuration](../docker/prometheus/prometheus.yml); changing
the Grafana host port changes its browser URL. This profile monitors the host
application, not automatically the independent container deployment below.

Database snapshots refresh every ten seconds; stale values and **No data** are
not proof of zero backlog/errors. Lifecycle counters reset on process restart
and are not the durable job ledger. Prometheus retains seven days of samples;
this is unrelated to job/idempotency retention. See [observability](observability.md)
for metric meanings, bounded labels, histogram limits, and dashboard details.

## Independent two-worker container environment

The [runtime image](../Dockerfile) copies the **already-verified executable JAR**;
it does not build with Maven. Verify first, then launch the standalone deployment:

```sh
mvn verify
docker compose -f compose.workers.yaml up -d --build --scale worker=2 --wait
curl --fail-with-body http://localhost:8081/actuator/health
```

The API may still be starting after Compose returns: there is no application
healthcheck in this deployment. Retry the health request until ready; the
dependency healthchecks alone do not establish API readiness.

[compose.workers.yaml](../compose.workers.yaml) is a **separate project** named
`job-platform-workers`. Always use it alone, never merge it with the development
Compose file. It has distinct PostgreSQL/RabbitMQ named volumes and does not
touch development jobs. The API publishes only `127.0.0.1:8081` by default
(`WORKERS_API_PORT` overrides it). Workers, database, and broker have **no
published host ports**; worker web servers remain internal to the Compose network.

One API container owns publication/retry/recovery; two worker containers execute
jobs. App containers run non-root with read-only roots, writable temporary
storage, one CPU, and 512MiB each. This differs from host-JVM benchmark resources;
container smoke results are correctness/packaging evidence, not capacity results.

Standalone local initialization credentials differ from development defaults:
PostgreSQL **job_platform / local-workers-development-only**; RabbitMQ
**job_platform / local-workers-rabbit-only**. They are still development-only
placeholders, not a production secret-management scheme.

For the isolated disposable packaging check, after verification:

```sh
python3 tools/container_smoke.py
```

It uses a unique project/random loopback port, verifies twenty jobs and two worker
identities plus container restrictions, then removes **only its own disposable**
containers/volumes. The persistent standalone and development volumes are separate.

## Reproduce experiments without faulting development services

With Java 21, Docker, Python 3.9+, and a freshly verified JAR/tooling suite:

```sh
python3 tools/benchmark.py
python3 tools/scaling.py
```

Each runner owns isolated dependencies/processes, records raw evidence under
ignored build output, separates arrival/completion/drain windows, and cleans up
only its resources. Do not run both suites simultaneously for comparisons:
they still share host CPU/memory with development services. Read the
[performance method](performance.md), [scaling method](scaling.md), and
[measured reports](benchmarks/2026-10-25-scaling.md) first. Generator drops,
observation failures, and invalid runs must remain visible; do not report them
as clean capacity results. First-start timing includes precommit time.

## VS Code tasks

Use **Terminal → Run Task** with the existing [.vscode/tasks.json](../.vscode/tasks.json):

- **Run platform (Java 21, macOS)** / **Run API only (Java 21, macOS)** select Java
  21 and start their required infrastructure. They do not set a loopback address;
  prefer the explicit command above or supply `SERVER_ADDRESS` in their environment.
- **Verify project (Java 21, macOS)** / **Verify benchmark runner** run the suites.
- **Check platform health** checks overall health and readiness on port 8080.
- **Start monitoring (Prometheus and Grafana)** / **Stop monitoring (preserve data)**
  manage monitoring without launching another app.
- The isolated benchmark/scaling, container build/smoke, standalone two-worker,
  and infrastructure start/stop tasks mirror the operations described above.

Java-selecting tasks are macOS-specific; the command-line workflows are portable
after selecting Java 21. Terminating the app task does not stop its dependencies.

## Stop without deleting saved data

Stop the host app with Ctrl+C or terminate its run task **before** stopping its
dependencies. Run only the stop commands for environments you started:

```sh
docker compose --profile monitoring stop prometheus grafana
docker compose stop postgres rabbitmq
docker compose -f compose.workers.yaml stop
```

These commands preserve named volumes and their saved jobs/history. Never use
volume deletion as an ordinary stop/reset step. Durable volumes are not backups;
deleted/corrupt storage and multi-host HA are outside the current recovery scope.
Jobs, attempts, results, outbox events, and idempotency outcomes have **no automatic
cleanup**. Plan retention before sustained operation; do not use these local
defaults as a production deployment guide.