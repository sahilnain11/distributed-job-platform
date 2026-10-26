# Contributing and verification

This repository is a distributed-systems learning project. Read the
[architecture](docs/design.md), [implementation walkthrough](docs/implementation-review.md)
and [scope/security limits](SECURITY.md) before proposing behavior changes.

## License status

No reuse license has been selected. Public visibility does not make this an
open-source-licensed project or grant permission to reuse/redistribute it. Discuss
licensing with the owner before submitting external code; do not assume a standard
open-source contribution license applies.

## Verify a change

Use Java 21, Maven 3.9+, Python 3.9+ and working Docker/Compose v2:

```sh
python3 tools/check_repository.py
python3 -m unittest discover -s tools -p 'test_*.py' -v
mvn --batch-mode --no-transfer-progress verify
```

Java integration tests use disposable real PostgreSQL/RabbitMQ. They must not
silently skip when Docker is unavailable. The Python unit suite mocks external
orchestration; full load/scaling runs are separate, resource-intensive experiments,
not a prerequisite for every small documentation change.

Changes to packaging also need the isolated smoke check after the verified build:

```sh
python3 tools/container_smoke.py
```

GitHub Actions runs these checks on Ubuntu with Java 21. A configured workflow
does not establish a passing hosted run; inspect its actual result. Keep failures,
dropped arrivals and measurement limits visible in reports.

## Preserve correctness boundaries

- Save acceptance, outbox intent and the frozen request outcome atomically.
- Expect duplicate publication/delivery; never remove ownership guards as an
  optimization or claim exactly-once external actions.
- Lock the job, recheck state/current attempt/fresh DB time, then commit related
  changes together. Do not hold the transaction during handler work or broker waits.
- ACK only after a committed outcome or authoritative duplicate/stale check.
  Database uncertainty is not permission to acknowledge or blindly rerun a handler.
- Give retries/recovery real transaction, concurrency, rollback and failure tests.
- Do not edit applied migrations, reset history/counters, delete volumes or fault
  the persistent development services to make a test pass.
- Keep UUIDs, request keys, payloads and raw error details out of metric labels.
- Keep production defaults separate from experimental overrides.

## Review notes

Keep changes focused. Describe the problem, the invariant preserved, tests actually
run and any limitations. Update public documentation when the contract changes.
Do not commit environment files, credentials, build output, raw logs, recordings
or machine-specific data. The repository guard is a focused heuristic check, not
a replacement for GitHub secret scanning or human review.