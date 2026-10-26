# Security and deployment scope

This is a local learning implementation, **not a production-hardened job service**.
Do not expose it on a public network or submit real secrets, personal data or
side-effecting business work through its demonstration handlers.

## Known boundaries

- No authentication, authorization or tenant isolation; callers share the
  `local-client` idempotency namespace. Job lookup includes stored payload/history.
- The default host application has no explicit loopback bind. The quick start
  recommends `SERVER_ADDRESS=127.0.0.1`; do not widen it on an untrusted network.
- Compose dependency/monitoring host ports bind to localhost. Worker deployment
  uses its own volumes and exposes only its API on localhost:8081.
- Passwords in the example configuration are deliberate **local development or
  disposable-test values**, not deployed production secrets. Never reuse them.
- No TLS termination, production secret rotation, redundant-storage HA or backup/
  restore guarantee is supplied. Named volumes are not backups.
- Payloads, attempts, results, saved request outcomes and outbox records have no
  automatic retention/cleanup policy. Monitor storage and privacy implications.
- Attempt ownership fences platform DB writes, not arbitrary external actions.
  Cancellation is cooperative, not a forced termination guarantee.
- Health, snapshots and metrics are observations, not proof of every job's
  progress or a security audit. Review [observability limits](docs/observability.md).

Container images use a pinned Java runtime/non-root user and the standalone app
uses resource limits/read-only roots. Those controls do not make the entire
deployment secure or remove the boundaries above. Update dependencies and image
digests deliberately after verification.

## Reporting a concern

Do **not** post credentials, payload data or exploit details in a public issue.
If the repository's **Security → Report a vulnerability** option is available,
use that private channel. Otherwise ask the owner for a private reporting method
without disclosing sensitive details publicly. No dedicated security support
window or response-time commitment is offered by this learning repository.

If a real credential was exposed, revoke/rotate it first. Removing a file from the
latest commit alone does not remove it from Git history or invalidate the secret.