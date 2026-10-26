package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sahil.jobplatform.persistence.JobPersistenceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "platform.reliability.heartbeat-ms=100", "platform.reliability.control-interval-ms=50"})
class ReliabilityStoreTests {

    @Container @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    @Autowired private JobExecutionStore store;
    @Autowired private JobPersistenceService persistence;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private ObjectMapper mapper;
    @Autowired private JobWorker worker;
    @Autowired private javax.sql.DataSource dataSource;
    @Autowired private com.sahil.jobplatform.observability.JobTelemetry telemetry;

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE idempotency_keys, outbox, job_results, job_attempts, job_payloads, jobs");
    }

    @Test
    void retryBackoffIsOneTwoFourSecondsAndTheFourthFailureEndsTheCycle() {
        UUID id = create("ECHO", 0, 0);
        for (int cycle = 1; cycle <= 4; cycle++) {
            var claim = store.claim(id, UUID.randomUUID(), 30);
            OffsetDateTime before = now();
            assertThat(store.failRetryably(id, claim.attemptId(), "TEMPORARY", "retryable failure", 1000)).isTrue();
            assertThat(jdbc.queryForObject("SELECT status FROM job_attempts WHERE attempt_id = ?", String.class, claim.attemptId()))
                    .isEqualTo("FAILED");
            assertThat(jdbc.queryForObject("SELECT attempt_count FROM jobs WHERE job_id = ?", Integer.class, id)).isEqualTo(cycle);
            if (cycle < 4) {
                var retry = jdbc.queryForObject("SELECT next_retry_at FROM jobs WHERE job_id = ?", OffsetDateTime.class, id);
                long expected = 1000L << (cycle - 1);
                assertThat(retry).isBetween(before.plus(Duration.ofMillis(expected)), now().plus(Duration.ofMillis(expected)));
                assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RETRYING");
                assertThat(store.queueRetry(id)).isFalse();
                makeRetryDue(id);
                assertThat(store.queueRetry(id)).isTrue();
                assertThat(store.queueRetry(id)).isFalse();
            } else {
                assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("FAILED");
                assertThat(jdbc.queryForObject("SELECT completed_at IS NOT NULL AND next_retry_at IS NULL FROM jobs WHERE job_id = ?",
                        Boolean.class, id)).isTrue();
                assertThat(store.queueRetry(id)).isFalse();
            }
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, id)).isEqualTo(4);
    }

    @Test
    void recoveredCrashExpiresHistoryAndSharesTheSameCycleBudget() {
        UUID id = create("ECHO", 0, 0);
        for (int cycle = 1; cycle <= 4; cycle++) {
            var abandoned = store.claim(id, UUID.randomUUID(), 30);
            expire(id);
            assertThat(store.expiredJobs(100)).contains(id);
            assertThat(store.recover(id, 1000)).isTrue();
            assertThat(store.recover(id, 1000)).isFalse();
            assertThat(store.complete(id, abandoned.attemptId(), abandoned.payload())).isFalse();
            assertThat(jdbc.queryForObject("SELECT status FROM job_attempts WHERE attempt_id = ?", String.class, abandoned.attemptId()))
                    .isEqualTo("EXPIRED");
            if (cycle < 4) {
                makeRetryDue(id);
                store.queueRetry(id);
            }
        }
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT cycle_attempt_count FROM jobs WHERE job_id = ?", Integer.class, id)).isEqualTo(4);
    }

    @Test
    void heartbeatExtendsOnlyTheCurrentUnexpiredLeaseAndRecoveryRechecksCandidates() {
        UUID id = create("ECHO", 0, 0);
        var attempt = store.claim(id, UUID.randomUUID(), 2);
        assertThat(store.supervise(id, attempt.attemptId(), 30, 300000, true, 1000))
                .isEqualTo(JobExecutionStore.Supervision.OWNED);
        assertThat(jdbc.queryForObject("SELECT lease_expiry > clock_timestamp() + interval '20 seconds' FROM jobs WHERE job_id = ?",
                Boolean.class, id)).isTrue();
        assertThat(store.recover(id, 1000)).isFalse();
        assertThat(store.supervise(id, UUID.randomUUID(), 30, 300000, true, 1000))
                .isEqualTo(JobExecutionStore.Supervision.LOST);
        expire(id);
        assertThat(store.supervise(id, attempt.attemptId(), 30, 300000, true, 1000))
                .isEqualTo(JobExecutionStore.Supervision.LOST);
        assertThat(store.recover(id, 1000)).isTrue();
    }

    @Test
    void runtimeLimitRecordsRetryableTimeoutRatherThanRenewingForever() {
        UUID id = create("ECHO", 0, 0);
        var attempt = store.claim(id, UUID.randomUUID(), 30);
        jdbc.update("UPDATE jobs SET started_at = clock_timestamp() - interval '301 seconds' WHERE job_id = ?", id);
        jdbc.update("UPDATE job_attempts SET started_at = clock_timestamp() - interval '301 seconds' WHERE attempt_id = ?", attempt.attemptId());
        assertThat(store.supervise(id, attempt.attemptId(), 30, 300000, true, 1000))
                .isEqualTo(JobExecutionStore.Supervision.TIMED_OUT);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RETRYING");
        assertThat(jdbc.queryForObject("SELECT failure_type FROM job_attempts WHERE attempt_id = ?", String.class, attempt.attemptId()))
                .isEqualTo("TIMEOUT");
        assertThat(store.complete(id, attempt.attemptId(), attempt.payload())).isFalse();
    }

    @Test
        void completionCannotBypassRuntimeDeadlineEvenIfAWatchdogCheckWasDelayed() {
        UUID id = create("ECHO", 0, 0);
        var attempt = store.claim(id, UUID.randomUUID(), 30);
        jdbc.update("UPDATE job_attempts SET started_at = clock_timestamp() - interval '301 seconds' WHERE attempt_id = ?",
            attempt.attemptId());
        assertThat(store.complete(id, attempt.attemptId(), attempt.payload())).isFalse();
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RETRYING");
        assertThat(jdbc.queryForObject("SELECT failure_type FROM job_attempts WHERE attempt_id = ?", String.class, attempt.attemptId()))
            .isEqualTo("TIMEOUT");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_results WHERE job_id = ?", Integer.class, id)).isZero();
        }

        @Test
    void competingSchedulersAndRecoveryManagersCommitEachTransitionOnlyOnce() throws Exception {
        UUID id = create("ECHO", 0, 0);
        store.claim(id, UUID.randomUUID(), 30);
        expire(id);
        assertOneWinner(() -> store.recover(id, 1000));
        makeRetryDue(id);
        assertOneWinner(() -> store.queueRetry(id));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, id)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
    }

    @Test
    void failedOutboxInsertRollsBackTheRetryQueueTransition() {
        UUID id = create("ECHO", 0, 0);
        var attempt = store.claim(id, UUID.randomUUID(), 30);
        store.failRetryably(id, attempt.attemptId(), "TEMP", "failure", 1000);
        makeRetryDue(id);
        jdbc.execute("ALTER TABLE outbox ADD CONSTRAINT test_no_retry_event CHECK (job_id <> '" + id + "'::uuid) NOT VALID");
        try { assertThat(catchThrowable(() -> store.queueRetry(id))).isNotNull(); }
        finally { jdbc.execute("ALTER TABLE outbox DROP CONSTRAINT test_no_retry_event"); }
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RETRYING");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
    }

    @Test
    void recoveryAttemptUpdateFailureRollsBackJobAndLeaseChanges() {
        UUID id = create("ECHO", 0, 0);
        store.claim(id, UUID.randomUUID(), 30);
        expire(id);
        jdbc.execute("ALTER TABLE job_attempts ADD CONSTRAINT test_no_expired CHECK (status <> 'EXPIRED')");
        try { assertThat(catchThrowable(() -> store.recover(id, 1000))).isNotNull(); }
        finally { jdbc.execute("ALTER TABLE job_attempts DROP CONSTRAINT test_no_expired"); }
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RUNNING");
        assertThat(jdbc.queryForObject("SELECT status FROM job_attempts WHERE job_id = ?", String.class, id)).isEqualTo("RUNNING");
    }

    @Test
    void heartbeatHasItsOwnConnectionsWhenEveryPrimaryPoolConnectionIsBusy() throws Exception {
        UUID id = create("ECHO", 0, 0);
        var attempt = store.claim(id, UUID.randomUUID(), 2);
        var held = new java.util.ArrayList<java.sql.Connection>();
        try {
            for (int i = 0; i < 10; i++) { held.add(dataSource.getConnection()); }
            assertThat(store.supervise(id, attempt.attemptId(), 30, 300000, true, 1000))
                    .isEqualTo(JobExecutionStore.Supervision.OWNED);
        } finally {
            for (var connection : held) { connection.close(); }
        }
        assertThat(jdbc.queryForObject("SELECT lease_expiry > clock_timestamp() + interval '20 seconds' FROM jobs WHERE job_id = ?",
                Boolean.class, id)).isTrue();
    }

    @Test
    void independentWatchdogActuallyInterruptsACancellableHandlerAndRecordsTimeout() throws Exception {
        UUID id = create("SIMULATED_WORK", 10000, 0);
        var settings = new ReliabilitySettings(50, 20, 200, 1000, false);
        try (var control = new java.util.concurrent.ScheduledThreadPoolExecutor(2);
             var execution = Executors.newSingleThreadExecutor()) {
            var supervisor = new AttemptSupervisor(store, settings, control);
            var supervisedWorker = new JobWorker(store, supervisor, settings, telemetry);
            var result = execution.submit(() -> supervisedWorker.process(id, 2));
            assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo(JobWorker.Outcome.ACK);
        }
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RETRYING");
        assertThat(jdbc.queryForObject("SELECT failure_type FROM job_attempts WHERE job_id = ?", String.class, id)).isEqualTo("TIMEOUT");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_results WHERE job_id = ?", Integer.class, id)).isZero();
    }

    @Test
    void invalidStoredSimulationFailsPermanentlyRatherThanSilentlyUsingZeroDefaults() {
        UUID id = UUID.randomUUID();
        persistence.createJob(id, "SIMULATED_WORK", mapper.createObjectNode());
        assertThat(worker.process(id, 30)).isEqualTo(JobWorker.Outcome.ACK);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT failure_type FROM job_attempts WHERE job_id = ?", String.class, id))
                .isEqualTo("INVALID_PAYLOAD");
        assertThat(jdbc.queryForObject("SELECT next_retry_at IS NULL FROM jobs WHERE job_id = ?", Boolean.class, id)).isTrue();
    }

    @Test
    void simulationUsesCurrentCycleAndThePlatformRecordsItsConfiguredFailures() {
        UUID id = create("SIMULATED_WORK", 0, 2);
        assertThat(worker.process(id, 30)).isEqualTo(JobWorker.Outcome.ACK);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RETRYING");
        makeRetryDue(id); store.queueRetry(id);
        worker.process(id, 30);
        makeRetryDue(id); store.queueRetry(id);
        worker.process(id, 30);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("COMPLETED");
        assertThat(jdbc.queryForList("SELECT status FROM job_attempts WHERE job_id = ? ORDER BY attempt_number", String.class, id))
                .containsExactly("FAILED", "FAILED", "COMPLETED");
    }

    private UUID create(String type, long duration, int failures) {
        UUID id = UUID.randomUUID();
        persistence.createJob(id, type, mapper.createObjectNode().put("durationMillis", duration).put("failFirstAttempts", failures));
        return id;
    }

    private void expire(UUID id) {
        jdbc.update("UPDATE jobs SET lease_expiry = clock_timestamp() - interval '1 second' WHERE job_id = ?", id);
    }

    private void makeRetryDue(UUID id) {
        jdbc.update("UPDATE jobs SET next_retry_at = clock_timestamp() - interval '1 second' WHERE job_id = ?", id);
    }

    private OffsetDateTime now() {
        return jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class);
    }

    private void assertOneWinner(java.util.concurrent.Callable<Boolean> operation) throws Exception {
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(2)) {
            java.util.concurrent.Callable<Boolean> wrapped = () -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) { throw new IllegalStateException("Test start timed out"); }
                return operation.call();
            };
            var a = pool.submit(wrapped); var b = pool.submit(wrapped);
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
            assertThat(List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
        }
    }
}