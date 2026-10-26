package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sahil.jobplatform.observability.JobTelemetry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

/** Concurrency-critical writes intentionally use SQL, not generic ORM saves. */
@Service
public class JobExecutionStore {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final SupervisionDatabase controls;
    private final ReliabilitySettings reliability;
    private final JobTelemetry telemetry;

    public JobExecutionStore(JdbcTemplate jdbc, ObjectMapper mapper, SupervisionDatabase controls,
                             ReliabilitySettings reliability, JobTelemetry telemetry) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.controls = controls;
        this.reliability = reliability;
        this.telemetry = telemetry;
    }

    public enum Decision { CLAIMED, DUPLICATE, MISSING }

    public record Claim(Decision decision, UUID jobId, UUID attemptId, String type, JsonNode payload, int cycleAttempt) { }

    @Transactional(timeout = 10)
    public Claim claim(UUID jobId, UUID workerId, int leaseSeconds) {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(workerId);
        if (leaseSeconds < 1) { throw new IllegalArgumentException("Lease must be positive"); }
        var rows = jdbc.query("""
                SELECT type, status, attempt_count, cycle_attempt_count, created_at FROM job_platform.jobs
                WHERE job_id = ? FOR UPDATE
                """, (row, number) -> new Candidate(row.getString(1), row.getString(2), row.getInt(3), row.getInt(4),
                row.getObject(5, OffsetDateTime.class)), jobId);
        if (rows.isEmpty()) { return new Claim(Decision.MISSING, jobId, null, null, null, 0); }
        Candidate job = rows.getFirst();
        if (!job.status().equals("QUEUED")) { return new Claim(Decision.DUPLICATE, jobId, null, job.type(), null, 0); }
        if (job.cycleCount() >= 4) { throw new IllegalStateException("Queued job has no cycle budget"); }
        UUID attemptId = UUID.randomUUID();
        OffsetDateTime start = Objects.requireNonNull(jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class));
        jdbc.update("""
                UPDATE job_platform.jobs SET status = 'RUNNING', attempt_count = attempt_count + 1,
                    cycle_attempt_count = cycle_attempt_count + 1, attempt_id = ?, started_at = ?,
                    lease_expiry = ?, next_retry_at = NULL, completed_at = NULL
                WHERE job_id = ? AND status = 'QUEUED'
                """, attemptId, start, start.plusSeconds(leaseSeconds), jobId);
        jdbc.update("""
                INSERT INTO job_platform.job_attempts
                    (attempt_id, job_id, attempt_number, worker_id, status, started_at)
                VALUES (?, ?, ?, ?, 'RUNNING', ?)
                """, attemptId, jobId, job.attemptCount() + 1, workerId, start);
        var payload = jdbc.query("SELECT payload::text FROM job_platform.job_payloads WHERE job_id = ?",
                (row, number) -> parse(row.getString(1)), jobId);
        telemetry.claimed(jobId, attemptId, job.type(), job.cycleCount() + 1,
                job.createdAt(), start, job.attemptCount() + 1);
        return new Claim(Decision.CLAIMED, jobId, attemptId, job.type(), payload.isEmpty() ? null : payload.getFirst(), job.cycleCount() + 1);
    }

    @Transactional(timeout = 10)
    public boolean complete(UUID jobId, UUID attemptId, JsonNode result) {
        Objects.requireNonNull(result);
        lock(jobId);
        int owned = jdbc.update("""
                UPDATE job_platform.jobs SET status = 'COMPLETED', completed_at = clock_timestamp(),
                    lease_expiry = NULL, next_retry_at = NULL
                WHERE job_id = ? AND status = 'RUNNING' AND attempt_id = ?
                    AND lease_expiry > clock_timestamp()
                    AND EXISTS (SELECT 1 FROM job_platform.job_attempts a
                        WHERE a.attempt_id = ? AND a.job_id = ?
                        AND a.started_at + (? * interval '1 millisecond') > clock_timestamp())
                """, jobId, attemptId, attemptId, jobId, reliability.maxRuntimeMs());
        if (owned == 0) {
            // A process pause can occur after the supervisor check but before
            // this write. Reject late success and record timeout if still owned.
            var owner = currentOwner(jobId);
            if (owns(owner, attemptId) && runtimeExceeded(owner, reliability.maxRuntimeMs())) {
                finishFailure(owner, jobId, attemptId, "FAILED", "TIMEOUT",
                        "Attempt runtime limit exceeded", reliability.retryBaseMs());
            }
            return false;
        }
        jdbc.update("""
                INSERT INTO job_platform.job_results (result_id, job_id, result, storage_type)
                VALUES (?, ?, ?::jsonb, 'DATABASE')
                """, UUID.randomUUID(), jobId, result.toString());
        requireAttemptUpdate(jdbc.query("""
                UPDATE job_platform.job_attempts SET status = 'COMPLETED', ended_at = clock_timestamp()
                WHERE attempt_id = ? AND job_id = ? AND status = 'RUNNING'
                RETURNING started_at, ended_at
                """, (row, number) -> {
                    telemetry.outcome(jobId, attemptId, "COMPLETED", "COMPLETED", null,
                            row.getObject(1, OffsetDateTime.class), row.getObject(2, OffsetDateTime.class));
                    return 1;
                }, attemptId, jobId).size());
        return true;
    }

    @Transactional(timeout = 10)
    public boolean failPermanently(UUID jobId, UUID attemptId, String failureType, String reason) {
        lock(jobId);
        int owned = jdbc.update("""
                UPDATE job_platform.jobs SET status = 'FAILED', completed_at = clock_timestamp(),
                    lease_expiry = NULL, next_retry_at = NULL
                WHERE job_id = ? AND status = 'RUNNING' AND attempt_id = ?
                    AND lease_expiry > clock_timestamp()
                """, jobId, attemptId);
        if (owned == 0) { return false; }
        requireAttemptUpdate(jdbc.query("""
                UPDATE job_platform.job_attempts SET status = 'FAILED', ended_at = clock_timestamp(),
                    failure_type = ?, failure_reason = ?
                WHERE attempt_id = ? AND job_id = ? AND status = 'RUNNING'
                RETURNING started_at, ended_at
                """, (row, number) -> {
                    telemetry.outcome(jobId, attemptId, "FAILED", "FAILED", failureType,
                            row.getObject(1, OffsetDateTime.class), row.getObject(2, OffsetDateTime.class));
                    return 1;
                }, failureType, reason, attemptId, jobId).size());
        return true;
    }

    @Transactional(readOnly = true)
    public boolean mayAcknowledgeOldAttempt(UUID jobId, UUID attemptId) {
        // Reconciliation after an ownership check returned false is not a new
        // execution claim. Queued work remains for its new outbox notification.
        return jdbc.query("""
                SELECT status, attempt_id, lease_expiry, clock_timestamp() AS db_now
                FROM job_platform.jobs WHERE job_id = ?
                """, (row, number) -> !"RUNNING".equals(row.getString("status"))
                || !attemptId.equals(row.getObject("attempt_id", UUID.class))
                || row.getObject("lease_expiry", OffsetDateTime.class) != null
                && !row.getObject("lease_expiry", OffsetDateTime.class).isAfter(row.getObject("db_now", OffsetDateTime.class)),
                jobId).stream().findFirst().orElse(false);
    }

    public enum Supervision { OWNED, LOST, TIMED_OUT }

    public Supervision supervise(UUID jobId, UUID attemptId, int leaseSeconds, long maxRuntimeMs,
                                 boolean renew, long backoffBaseMs) {
        return controls.execute(control -> {
            var owner = currentOwner(control, jobId);
            if (!owns(owner, attemptId)) { return Supervision.LOST; }
            if (runtimeExceeded(owner, maxRuntimeMs)) {
                finishFailure(control, owner, jobId, attemptId, "FAILED", "TIMEOUT", "Attempt runtime limit exceeded", backoffBaseMs);
                return Supervision.TIMED_OUT;
            }
            if (renew) {
                control.update("UPDATE job_platform.jobs SET lease_expiry = ? WHERE job_id = ?",
                        owner.now().plusSeconds(leaseSeconds), jobId);
                telemetry.heartbeat(jobId, attemptId);
            }
            return Supervision.OWNED;
        });
    }

    @Transactional(timeout = 10)
    public boolean failRetryably(UUID jobId, UUID attemptId, String type, String reason, long backoffBaseMs) {
        var owner = currentOwner(jobId);
        if (!owns(owner, attemptId)) { return false; }
        boolean timedOut = runtimeExceeded(owner, reliability.maxRuntimeMs());
        finishFailure(owner, jobId, attemptId, "FAILED", timedOut ? "TIMEOUT" : type,
            timedOut ? "Attempt runtime limit exceeded" : reason, backoffBaseMs);
        return true;
    }

    @Transactional(readOnly = true)
    public java.util.List<UUID> dueRetries(int limit) {
        return jdbc.query("""
                SELECT job_id FROM job_platform.jobs
                WHERE status = 'RETRYING' AND next_retry_at <= clock_timestamp()
                ORDER BY next_retry_at, job_id LIMIT ?
                """, (row, number) -> row.getObject(1, UUID.class), limit);
    }

    @Transactional(timeout = 10)
    public boolean queueRetry(UUID jobId) {
        lock(jobId);
        int changed = jdbc.update("""
                UPDATE job_platform.jobs SET status = 'QUEUED', next_retry_at = NULL
                WHERE job_id = ? AND status = 'RETRYING' AND next_retry_at <= clock_timestamp()
                """, jobId);
        if (changed == 0) { return false; }
        createEvent(jobId);
        telemetry.retry(jobId, false);
        return true;
    }

    @Transactional(readOnly = true)
    public java.util.List<UUID> expiredJobs(int limit) {
        return jdbc.query("""
                SELECT job_id FROM job_platform.jobs
                WHERE status = 'RUNNING' AND lease_expiry <= clock_timestamp()
                ORDER BY lease_expiry, job_id LIMIT ?
                """, (row, number) -> row.getObject(1, UUID.class), limit);
    }

    @Transactional(timeout = 10)
    public boolean recover(UUID jobId, long backoffBaseMs) {
        var owner = currentOwner(jobId);
        if (owner == null || !"RUNNING".equals(owner.status()) || owner.attemptId() == null
                || owner.leaseExpiry() == null || owner.leaseExpiry().isAfter(owner.now())) { return false; }
        finishFailure(owner, jobId, owner.attemptId(), "EXPIRED", "LEASE_EXPIRED",
                "Worker lease expired before a terminal outcome was recorded", backoffBaseMs);
        return true;
    }

    private Owner currentOwner(UUID jobId) {
        return currentOwner(jdbc, jobId);
    }

    private Owner currentOwner(JdbcTemplate connection, UUID jobId) {
        connection.query("SELECT job_id FROM job_platform.jobs WHERE job_id = ? FOR UPDATE",
                (row, number) -> row.getObject(1, UUID.class), jobId);
        return connection.query("""
                SELECT j.status, j.attempt_id, j.cycle_attempt_count, j.lease_expiry, a.started_at,
                    clock_timestamp() AS db_now FROM job_platform.jobs j
                LEFT JOIN job_platform.job_attempts a ON a.attempt_id = j.attempt_id AND a.job_id = j.job_id
                WHERE j.job_id = ?
                """, (row, number) -> new Owner(row.getString(1), row.getObject(2, UUID.class), row.getInt(3),
                row.getObject(4, OffsetDateTime.class), row.getObject(5, OffsetDateTime.class),
                row.getObject(6, OffsetDateTime.class)), jobId).stream().findFirst().orElse(null);
    }

    private boolean owns(Owner owner, UUID attemptId) {
        return owner != null && "RUNNING".equals(owner.status()) && attemptId.equals(owner.attemptId())
                && owner.leaseExpiry() != null && owner.leaseExpiry().isAfter(owner.now());
    }

    private boolean runtimeExceeded(Owner owner, long limitMs) {
        if (owner.startedAt() == null) { throw new IllegalStateException("Running job has no attempt start time"); }
        return !owner.startedAt().plus(java.time.Duration.ofMillis(limitMs)).isAfter(owner.now());
    }

    private void finishFailure(Owner owner, UUID jobId, UUID attemptId, String attemptStatus,
                               String type, String reason, long backoffBaseMs) {
        finishFailure(jdbc, owner, jobId, attemptId, attemptStatus, type, reason, backoffBaseMs);
    }

    private void finishFailure(JdbcTemplate connection, Owner owner, UUID jobId, UUID attemptId, String attemptStatus,
                               String type, String reason, long backoffBaseMs) {
        if (owner.cycleCount() < 1 || owner.cycleCount() > 4) {
            throw new IllegalStateException("Running attempt has invalid cycle accounting");
        }
        boolean retry = owner.cycleCount() < 4;
        long delay = Math.multiplyExact(backoffBaseMs, 1L << (owner.cycleCount() - 1));
        connection.update("""
                UPDATE job_platform.jobs SET status = ?, lease_expiry = NULL,
                    next_retry_at = ?, completed_at = ? WHERE job_id = ?
                """, retry ? "RETRYING" : "FAILED",
                retry ? owner.now().plus(java.time.Duration.ofMillis(delay)) : null,
                retry ? null : owner.now(), jobId);
        requireAttemptUpdate(connection.query("""
                UPDATE job_platform.job_attempts SET status = ?, ended_at = ?, failure_type = ?, failure_reason = ?
                WHERE attempt_id = ? AND job_id = ? AND status = 'RUNNING'
                RETURNING started_at, ended_at
                """, (row, number) -> {
                    telemetry.outcome(jobId, attemptId, attemptStatus, retry ? "RETRYING" : "FAILED", type,
                            row.getObject(1, OffsetDateTime.class), row.getObject(2, OffsetDateTime.class));
                    return 1;
                }, attemptStatus, owner.now(), type, reason, attemptId, jobId).size());
    }

    private void createEvent(UUID jobId) {
        jdbc.update("INSERT INTO job_platform.outbox (outbox_id, job_id) VALUES (?, ?)", UUID.randomUUID(), jobId);
    }

    private void lock(UUID jobId) {
        jdbc.query("SELECT job_id FROM job_platform.jobs WHERE job_id = ? FOR UPDATE",
                (row, number) -> row.getObject(1, UUID.class), jobId);
    }

    private void requireAttemptUpdate(int count) {
        if (count != 1) { throw new IllegalStateException("Attempt transition did not match current job ownership"); }
    }

    private JsonNode parse(String json) {
        try { return mapper.readTree(json); }
        catch (JsonProcessingException failure) { throw new IllegalStateException("Invalid stored job payload", failure); }
    }

    private record Candidate(String type, String status, int attemptCount, int cycleCount, OffsetDateTime createdAt) { }

    private record Owner(String status, UUID attemptId, int cycleCount, OffsetDateTime leaseExpiry,
                         OffsetDateTime startedAt, OffsetDateTime now) { }
}