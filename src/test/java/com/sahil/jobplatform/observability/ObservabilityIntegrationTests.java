package com.sahil.jobplatform.observability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sahil.jobplatform.api.JobSubmissionService;
import com.sahil.jobplatform.messaging.JobExecutionStore;
import com.sahil.jobplatform.messaging.JobWorker;
import com.sahil.jobplatform.persistence.JobPersistenceService;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@Testcontainers
@AutoConfigureObservability
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "platform.observability.snapshot-interval-ms=3600000")
class ObservabilityIntegrationTests {

    @Container @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    @Autowired private MeterRegistry metrics;
    @Autowired private JobPersistenceService persistence;
    @Autowired private JobExecutionStore execution;
    @Autowired private JobWorker worker;
    @Autowired private JobSubmissionService submissions;
    @Autowired private DatabaseMetricsSnapshot snapshot;
    @Autowired private PlatformTransactionManager transactions;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private TestRestTemplate http;
    @Autowired private ObjectMapper mapper;

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE idempotency_keys, outbox, job_results, job_attempts, job_payloads, jobs");
    }

    @Test
    void committedLifecycleCountsAndDurationsMatchActualExecution() {
        double accepted = count("jobs.accepted", "type", "ECHO");
        double started = count("jobs.attempts.started", "type", "ECHO");
        double completed = count("jobs.terminal", "outcome", "COMPLETED");
        UUID id = create("ECHO");
        assertThat(worker.process(id, 30)).isEqualTo(JobWorker.Outcome.ACK);
        assertThat(count("jobs.accepted", "type", "ECHO")).isEqualTo(accepted + 1);
        assertThat(count("jobs.attempts.started", "type", "ECHO")).isEqualTo(started + 1);
        assertThat(count("jobs.terminal", "outcome", "COMPLETED")).isEqualTo(completed + 1);
        assertThat(metrics.find("jobs.first.start.delay").tag("type", "ECHO").timer().count()).isGreaterThan(0);
        assertThat(metrics.find("jobs.attempt.duration").tag("outcome", "COMPLETED").timer().count()).isGreaterThan(0);
        assertThat(metrics.get("jobs.worker.active").gauge().value()).isZero();
    }

    @Test
    void outerRollbackDoesNotCountOrLogAcceptedOrStartedWork(CapturedOutput output) {
        UUID id = UUID.randomUUID();
        double accepted = count("jobs.accepted", "type", "ECHO");
        double started = count("jobs.attempts.started", "type", "ECHO");
        Throwable rollback = catchThrowable(() -> new TransactionTemplate(transactions).executeWithoutResult(tx -> {
            persistence.createJob(id, "ECHO", mapper.createObjectNode());
            execution.claim(id, UUID.randomUUID(), 30);
            throw new IllegalStateException("Deliberate rollback after flushed claim");
        }));
        assertThat(rollback).isNotNull();
        assertThat(count("jobs.accepted", "type", "ECHO")).isEqualTo(accepted);
        assertThat(count("jobs.attempts.started", "type", "ECHO")).isEqualTo(started);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs WHERE job_id=?", Integer.class, id)).isZero();
        assertThat(output.getOut()).doesNotContain(id.toString());
    }

    @Test
    void outcomeRollbackDoesNotIncrementTerminalCounter() {
        UUID id = create("ECHO");
        var claim = execution.claim(id, UUID.randomUUID(), 30);
        double completed = count("jobs.terminal", "outcome", "COMPLETED");
        jdbc.execute("ALTER TABLE job_attempts ADD CONSTRAINT test_no_observed_completion CHECK (status <> 'COMPLETED')");
        try { assertThat(catchThrowable(() -> execution.complete(id, claim.attemptId(), claim.payload()))).isNotNull(); }
        finally { jdbc.execute("ALTER TABLE job_attempts DROP CONSTRAINT test_no_observed_completion"); }
        assertThat(count("jobs.terminal", "outcome", "COMPLETED")).isEqualTo(completed);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id=?", String.class, id)).isEqualTo("RUNNING");
    }

    @Test
    void supervisionPoolTimeoutOutcomeAlsoEmitsOnlyAfterItsCommit() {
        UUID id = create("ECHO");
        var claim = execution.claim(id, UUID.randomUUID(), 30);
        jdbc.update("UPDATE job_attempts SET started_at=clock_timestamp()-interval '301 seconds' WHERE attempt_id=?", claim.attemptId());
        double timeouts = count("jobs.attempt.outcomes", "outcome", "FAILED", "failure", "TIMEOUT");
        assertThat(execution.supervise(id, claim.attemptId(), 30, 300000, true, 1000))
                .isEqualTo(JobExecutionStore.Supervision.TIMED_OUT);
        assertThat(count("jobs.attempt.outcomes", "outcome", "FAILED", "failure", "TIMEOUT")).isEqualTo(timeouts + 1);
        snapshot.refresh();
        assertThat(metrics.get("jobs.state").tag("status", "RETRYING").gauge().value()).isEqualTo(1);
    }

    @Test
    void replayCountsRequestsButNeverAcceptsAnotherJobOrManualCycle() {
        String key = UUID.randomUUID().toString();
        var request = mapper.createObjectNode().put("type", "ECHO");
        request.putObject("payload").put("message", "replay observation");
        double accepted = count("jobs.accepted", "type", "ECHO");
        double replay = count("jobs.api.outcomes", "operation", "SUBMIT_JOB", "replay", "true", "status", "202");
        var first = submissions.submit(key, request, false);
        assertThat(submissions.submit(key, request, false).body()).isEqualTo(first.body());
        assertThat(count("jobs.accepted", "type", "ECHO")).isEqualTo(accepted + 1);
        assertThat(count("jobs.api.outcomes", "operation", "SUBMIT_JOB", "replay", "true", "status", "202")).isEqualTo(replay + 1);
        UUID id = UUID.fromString(first.body().path("jobId").asText());
        var claim = execution.claim(id, UUID.randomUUID(), 30);
        execution.failPermanently(id, claim.attemptId(), "INVALID_PAYLOAD", "test permanent outcome");
        double manual = count("jobs.retry.events", "kind", "manual");
        String retryKey = UUID.randomUUID().toString();
        submissions.retry(retryKey, id, null);
        submissions.retry(retryKey, id, null);
        assertThat(count("jobs.retry.events", "kind", "manual")).isEqualTo(manual + 1);
        assertThat(metrics.find("jobs.api.outcomes").counters()).allSatisfy(counter ->
                assertThat(counter.getId().getTag("operation")).isIn("SUBMIT_JOB", "SUBMIT_BATCH", "RETRY_JOB"));
    }

    @Test
    void expiryAndAutomaticRetryCountOnceWhileRenewalAndFirstStartKeepTheirMeaning() {
        UUID id = create("ECHO");
        var claim = execution.claim(id, UUID.randomUUID(), 30);
        long starts = metrics.get("jobs.first.start.delay").tag("type", "ECHO").timer().count();
        double renewed = count("jobs.heartbeat.renewed");
        execution.supervise(id, claim.attemptId(), 30, 300000, true, 1000);
        assertThat(count("jobs.heartbeat.renewed")).isEqualTo(renewed + 1);
        jdbc.update("UPDATE jobs SET lease_expiry=clock_timestamp()-interval '1 second' WHERE job_id=?", id);
        double expired = count("jobs.attempt.outcomes", "outcome", "EXPIRED", "failure", "LEASE_EXPIRED");
        assertThat(execution.recover(id, 1000)).isTrue();
        assertThat(execution.recover(id, 1000)).isFalse();
        assertThat(count("jobs.attempt.outcomes", "outcome", "EXPIRED", "failure", "LEASE_EXPIRED")).isEqualTo(expired + 1);
        jdbc.update("UPDATE jobs SET next_retry_at=clock_timestamp()-interval '1 second' WHERE job_id=?", id);
        double retries = count("jobs.retry.events", "kind", "automatic");
        assertThat(execution.queueRetry(id)).isTrue();
        assertThat(execution.queueRetry(id)).isFalse();
        assertThat(count("jobs.retry.events", "kind", "automatic")).isEqualTo(retries + 1);
        worker.process(id, 30);
        assertThat(metrics.get("jobs.first.start.delay").tag("type", "ECHO").timer().count()).isEqualTo(starts);
    }

    @Test
    void failedControlTransactionAndRetryOutboxWriteNeverEmitSuccessfulTransitions() {
        UUID id = create("ECHO");
        var claim = execution.claim(id, UUID.randomUUID(), 30);
        jdbc.update("UPDATE job_attempts SET started_at=clock_timestamp()-interval '301 seconds' WHERE attempt_id=?", claim.attemptId());
        double timeouts = count("jobs.attempt.outcomes", "outcome", "FAILED", "failure", "TIMEOUT");
        jdbc.execute("ALTER TABLE job_attempts ADD CONSTRAINT test_no_observed_timeout CHECK (status <> 'FAILED')");
        try { assertThat(catchThrowable(() -> execution.supervise(id, claim.attemptId(), 30, 300000, true, 1000))).isNotNull(); }
        finally { jdbc.execute("ALTER TABLE job_attempts DROP CONSTRAINT test_no_observed_timeout"); }
        assertThat(count("jobs.attempt.outcomes", "outcome", "FAILED", "failure", "TIMEOUT")).isEqualTo(timeouts);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id=?", String.class, id)).isEqualTo("RUNNING");
        jdbc.update("UPDATE jobs SET lease_expiry=clock_timestamp()-interval '1 second' WHERE job_id=?", id);
        execution.recover(id, 1000);
        jdbc.update("UPDATE jobs SET next_retry_at=clock_timestamp()-interval '1 second' WHERE job_id=?", id);
        double retries = count("jobs.retry.events", "kind", "automatic");
        jdbc.execute("ALTER TABLE outbox ADD CONSTRAINT test_no_observed_retry CHECK (published) NOT VALID");
        try { assertThat(catchThrowable(() -> execution.queueRetry(id))).isNotNull(); }
        finally { jdbc.execute("ALTER TABLE outbox DROP CONSTRAINT test_no_observed_retry"); }
        assertThat(count("jobs.retry.events", "kind", "automatic")).isEqualTo(retries);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id=?", String.class, id)).isEqualTo("RETRYING");
    }

    @Test
    void snapshotIsCachedAndFailureKeepsLastKnownValuesWithAnExplicitBadFreshnessSignal() {
        create("ECHO");
        snapshot.refresh();
        assertThat(metrics.get("jobs.state").tag("status", "QUEUED").gauge().value()).isEqualTo(1);
        assertThat(metrics.get("jobs.outbox.pending").gauge().value()).isEqualTo(1);
        assertThat(metrics.get("jobs.snapshot.up").gauge().value()).isEqualTo(1);
        double lastGood = metrics.get("jobs.snapshot.last.success").gauge().value();
        create("ECHO");
        assertThat(metrics.get("jobs.state").tag("status", "QUEUED").gauge().value()).isEqualTo(1);
        snapshot.refresh();
        assertThat(metrics.get("jobs.state").tag("status", "QUEUED").gauge().value()).isEqualTo(2);
        lastGood = metrics.get("jobs.snapshot.last.success").gauge().value();
        jdbc.execute("ALTER TABLE job_platform.outbox RENAME TO temporarily_hidden_outbox");
        try {
            snapshot.refresh();
            assertThat(metrics.get("jobs.snapshot.up").gauge().value()).isZero();
            assertThat(metrics.get("jobs.snapshot.last.success").gauge().value()).isEqualTo(lastGood);
            assertThat(metrics.get("jobs.state").tag("status", "QUEUED").gauge().value()).isEqualTo(2);
            // Gauge reads and HTTP scrapes work even while the table is missing.
            assertThat(http.getForEntity("/actuator/prometheus", String.class).getStatusCode().value()).isEqualTo(200);
        } finally { jdbc.execute("ALTER TABLE job_platform.temporarily_hidden_outbox RENAME TO outbox"); }
        snapshot.refresh();
        assertThat(metrics.get("jobs.snapshot.up").gauge().value()).isEqualTo(1);
    }

    @Test
    void labelsAreBoundedAndStructuredLogsContainIdsButNotPayloadOrRequestKeys(CapturedOutput output) throws Exception {
        String secret = "payload-secret-" + UUID.randomUUID();
        String key = "client-key-secret-" + UUID.randomUUID();
        var request = mapper.createObjectNode().put("type", "ECHO");
        request.putObject("payload").put("message", secret);
        var first = submissions.submit(key, request, false);
        UUID id = UUID.fromString(first.body().get("jobId").asText());
        worker.process(id, 30);
        assertThat(submissions.submit(key, request, false).body()).isEqualTo(first.body());
        for (int i = 0; i < 3; i++) { create("dynamic-untrusted-type-" + UUID.randomUUID()); }
        assertThat(metrics.find("jobs.accepted").counters().stream().map(counter -> counter.getId().getTag("type")))
                .contains("ECHO", "OTHER").allMatch(type -> java.util.Set.of("ECHO", "SIMULATED_WORK", "OTHER").contains(type));
        assertThat(metrics.getMeters()).allSatisfy(meter -> meter.getId().getTags().forEach(tag -> {
            assertThat(tag.getKey()).isNotIn("jobId", "attemptId", "workerId", "idempotencyKey", "operation_scope");
            assertThat(tag.getValue()).doesNotContain(secret, key, id.toString());
        }));
        var events = output.getOut().lines().filter(line -> line.startsWith("{") && line.contains("job_accepted")).toList();
        assertThat(events).isNotEmpty();
        assertThat(events.stream().map(line -> {
            try { return mapper.readTree(line); } catch (Exception e) { throw new IllegalStateException(e); }
        }).anyMatch(event -> event.path("jobId").asText().equals(id.toString()))).isTrue();
        assertThat(output.getOut()).doesNotContain(secret, key);
    }

    @Test
    void prometheusExportsLifecycleAndPoolMetricsWithoutExposingOtherActuatorEndpoints() {
        UUID id = create("ECHO"); worker.process(id, 30); snapshot.refresh();
        var response = http.getForEntity("/actuator/prometheus", String.class);
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).contains("jobs_accepted_total", "jobs_state", "jobs_worker_active",
                "jobs_attempt_duration_seconds", "jobs_first_start_delay_seconds", "jobs_snapshot_up",
                "hikaricp_connections", "attempt-control");
        // Histogram ranges must cover the five-minute runtime, not the default
        // 30-second ceiling that would hide longer valid attempts in +Inf.
        assertThat(response.getBody()).contains("le=\"600.0\"", "le=\"3600.0\"");
        assertThat(http.getForEntity("/actuator/env", String.class).getStatusCode().value()).isEqualTo(404);
        assertThat(http.getForEntity("/actuator/metrics", String.class).getStatusCode().value()).isEqualTo(404);
    }

    private UUID create(String type) {
        UUID id = UUID.randomUUID();
        persistence.createJob(id, type, mapper.createObjectNode().put("message", "telemetry test"));
        return id;
    }

    private double count(String metric, String... tags) {
        var counter = metrics.find(metric).tags(tags).counter();
        return counter == null ? 0 : counter.count();
    }
}