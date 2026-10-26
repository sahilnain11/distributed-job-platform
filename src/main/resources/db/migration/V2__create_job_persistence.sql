CREATE TABLE job_platform.jobs (
    job_id UUID PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL DEFAULT 'QUEUED',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    cycle_attempt_count INTEGER NOT NULL DEFAULT 0,
    attempt_id UUID,
    lease_expiry TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT jobs_status_check CHECK (status IN ('QUEUED', 'RUNNING', 'RETRYING', 'COMPLETED', 'FAILED')),
    CONSTRAINT jobs_attempt_count_check CHECK (attempt_count >= 0),
    CONSTRAINT jobs_cycle_attempt_count_check CHECK (cycle_attempt_count BETWEEN 0 AND 4),
    CONSTRAINT jobs_cycle_within_lifetime_check CHECK (cycle_attempt_count <= attempt_count)
);

CREATE INDEX jobs_retry_idx ON job_platform.jobs (status, next_retry_at);
CREATE INDEX jobs_lease_idx ON job_platform.jobs (status, lease_expiry);

CREATE TABLE job_platform.job_payloads (
    payload_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE REFERENCES job_platform.jobs (job_id),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_platform.job_attempts (
    attempt_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES job_platform.jobs (job_id),
    attempt_number INTEGER NOT NULL,
    worker_id UUID NOT NULL,
    status VARCHAR(255) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    failure_type VARCHAR(255),
    failure_reason TEXT,
    CONSTRAINT job_attempts_number_unique UNIQUE (job_id, attempt_number),
    CONSTRAINT job_attempts_number_check CHECK (attempt_number > 0),
    CONSTRAINT job_attempts_status_check CHECK (status IN ('RUNNING', 'FAILED', 'COMPLETED', 'EXPIRED'))
);

CREATE UNIQUE INDEX one_running_attempt_per_job
    ON job_platform.job_attempts (job_id) WHERE status = 'RUNNING';

CREATE TABLE job_platform.job_results (
    result_id UUID PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE REFERENCES job_platform.jobs (job_id),
    result JSONB,
    storage_type VARCHAR(255) NOT NULL,
    location TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_platform.idempotency_keys (
    client_id TEXT NOT NULL,
    operation_scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_status INTEGER NOT NULL,
    response_body JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id, operation_scope, idempotency_key)
);

CREATE TABLE job_platform.outbox (
    outbox_id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES job_platform.jobs (job_id),
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx
    ON job_platform.outbox (created_at, outbox_id) WHERE published = FALSE;