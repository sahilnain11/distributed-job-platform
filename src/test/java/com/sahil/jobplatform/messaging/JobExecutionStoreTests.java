package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sahil.jobplatform.persistence.JobPersistenceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class JobExecutionStoreTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    @Autowired private JobExecutionStore store;
    @Autowired private JobWorker worker;
    @Autowired private JobPersistenceService persistence;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private ObjectMapper mapper;
    @Autowired private PlatformTransactionManager transactions;

    @BeforeEach
    void clearTestDatabase() {
        jdbc.execute("TRUNCATE idempotency_keys, outbox, job_results, job_attempts, job_payloads, jobs");
    }

    @Test
    void claimCommitsAttemptCountersAndLeaseBeforeCompletionAndResult() {
        UUID id = create();
        var claim = store.claim(id, UUID.randomUUID(), 30);
        assertThat(claim.decision()).isEqualTo(JobExecutionStore.Decision.CLAIMED);
        assertThat(jdbc.queryForMap("SELECT status, attempt_count, cycle_attempt_count FROM jobs WHERE job_id = ?", id))
                .containsEntry("status", "RUNNING").containsEntry("attempt_count", 1).containsEntry("cycle_attempt_count", 1);
        assertThat(jdbc.queryForObject("""
                SELECT j.started_at = a.started_at AND j.lease_expiry > clock_timestamp()
                FROM jobs j JOIN job_attempts a ON j.attempt_id = a.attempt_id WHERE j.job_id = ?
                """, Boolean.class, id)).isTrue();
        assertThat(store.complete(id, claim.attemptId(), claim.payload())).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("COMPLETED");
        assertThat(jdbc.queryForObject("SELECT status FROM job_attempts WHERE attempt_id = ?", String.class, claim.attemptId()))
                .isEqualTo("COMPLETED");
        assertThat(jdbc.queryForObject("SELECT lease_expiry IS NULL AND completed_at IS NOT NULL FROM jobs WHERE job_id = ?",
                Boolean.class, id)).isTrue();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_results WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
        assertThat(store.claim(id, UUID.randomUUID(), 30).decision()).isEqualTo(JobExecutionStore.Decision.DUPLICATE);
    }

    @Test
    void simultaneousClaimsProduceOneAttemptAndOneOwner() throws Exception {
        UUID id = create();
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        try (var threads = Executors.newFixedThreadPool(2)) {
            java.util.concurrent.Callable<JobExecutionStore.Claim> operation = () -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) { throw new IllegalStateException("Test start timed out"); }
                return store.claim(id, UUID.randomUUID(), 30);
            };
            var a = threads.submit(operation);
            var b = threads.submit(operation);
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(java.util.List.of(a.get(20, TimeUnit.SECONDS).decision(), b.get(20, TimeUnit.SECONDS).decision()))
                    .containsExactlyInAnyOrder(JobExecutionStore.Decision.CLAIMED, JobExecutionStore.Decision.DUPLICATE);
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM jobs WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
    }

    @Test
    void attemptInsertFailureRollsBackTheProvisionalJobClaim() {
        UUID id = create();
        jdbc.execute("ALTER TABLE job_attempts ADD CONSTRAINT test_deny_claim CHECK (job_id <> '" + id + "'::uuid)");
        try {
            assertThat(catchThrowable(() -> store.claim(id, UUID.randomUUID(), 30))).isNotNull();
        } finally {
            jdbc.execute("ALTER TABLE job_attempts DROP CONSTRAINT test_deny_claim");
        }
        assertThat(jdbc.queryForMap("SELECT status, attempt_count, cycle_attempt_count FROM jobs WHERE job_id = ?", id))
                .containsEntry("status", "QUEUED").containsEntry("attempt_count", 0).containsEntry("cycle_attempt_count", 0);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts", Integer.class)).isZero();
    }

    @Test
    void expiredOwnerCannotSaveSuccessOrFailureBeforeRecoveryRuns() {
        UUID id = create();
        var claim = store.claim(id, UUID.randomUUID(), 30);
        jdbc.update("UPDATE jobs SET lease_expiry = clock_timestamp() - interval '1 second' WHERE job_id = ?", id);
        assertThat(store.complete(id, claim.attemptId(), claim.payload())).isFalse();
        assertThat(store.failPermanently(id, claim.attemptId(), "OLD", "stale error")).isFalse();
        assertThat(store.mayAcknowledgeOldAttempt(id, claim.attemptId())).isTrue();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_results", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RUNNING");
    }

    @Test
    void resumedOldAttemptCannotOverwriteReplacementEvenWithValidNewLease() {
        UUID id = create();
        var old = store.claim(id, UUID.randomUUID(), 30);
        // Controlled recovery fixture, not an implemented recovery manager.
        jdbc.update("UPDATE job_attempts SET status = 'EXPIRED', ended_at = clock_timestamp() WHERE attempt_id = ?", old.attemptId());
        jdbc.update("UPDATE jobs SET status = 'QUEUED', lease_expiry = NULL WHERE job_id = ?", id);
        var current = store.claim(id, UUID.randomUUID(), 30);
        assertThat(store.complete(id, old.attemptId(), old.payload())).isFalse();
        assertThat(store.failPermanently(id, old.attemptId(), "OLD", "old failure")).isFalse();
        assertThat(store.complete(id, current.attemptId(), current.payload())).isTrue();
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM jobs WHERE job_id = ?", Integer.class, id)).isEqualTo(2);
    }

    @Test
    void resultFailureRollsBackJobCompletionAndLeavesAttemptRunning() {
        UUID id = create();
        var claim = store.claim(id, UUID.randomUUID(), 30);
        jdbc.execute("ALTER TABLE job_results ADD CONSTRAINT test_deny_result CHECK (job_id <> '" + id + "'::uuid)");
        try {
            assertThat(catchThrowable(() -> store.complete(id, claim.attemptId(), claim.payload()))).isNotNull();
        } finally { jdbc.execute("ALTER TABLE job_results DROP CONSTRAINT test_deny_result"); }
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RUNNING");
        assertThat(jdbc.queryForObject("SELECT status FROM job_attempts WHERE attempt_id = ?", String.class, claim.attemptId()))
                .isEqualTo("RUNNING");
        assertThat(store.complete(id, claim.attemptId(), claim.payload())).isTrue();
    }

    @Test
    void attemptCompletionFailureRollsBackResultAndJobTogether() {
        UUID id = create();
        var claim = store.claim(id, UUID.randomUUID(), 30);
        jdbc.execute("ALTER TABLE job_attempts ADD CONSTRAINT test_deny_complete CHECK (status <> 'COMPLETED')");
        try {
            assertThat(catchThrowable(() -> store.complete(id, claim.attemptId(), claim.payload()))).isNotNull();
        } finally { jdbc.execute("ALTER TABLE job_attempts DROP CONSTRAINT test_deny_complete"); }
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RUNNING");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_results", Integer.class)).isZero();
    }

    @Test
    void completionChecksFreshTimeAfterWaitingForTheJobLock() throws Exception {
        UUID id = create();
        var claim = store.claim(id, UUID.randomUUID(), 30);
        CountDownLatch lockHeld = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var threads = Executors.newFixedThreadPool(2)) {
            var locker = threads.submit(() -> new TransactionTemplate(transactions).executeWithoutResult(tx -> {
                jdbc.queryForObject("SELECT job_id FROM jobs WHERE job_id = ? FOR UPDATE", UUID.class, id);
                lockHeld.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) { throw new IllegalStateException("Test release timed out"); }
                } catch (InterruptedException exception) { throw new IllegalStateException(exception); }
                jdbc.update("UPDATE jobs SET lease_expiry = clock_timestamp() WHERE job_id = ?", id);
            }));
            assertThat(lockHeld.await(10, TimeUnit.SECONDS)).isTrue();
            var completion = threads.submit(() -> store.complete(id, claim.attemptId(), claim.payload()));
            release.countDown();
            locker.get(20, TimeUnit.SECONDS);
            assertThat(completion.get(20, TimeUnit.SECONDS)).isFalse();
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_results", Integer.class)).isZero();
    }

    @Test
    void permanentFailureRecordsAttemptAndJobBeforeAcknowledgement() {
        UUID id = UUID.randomUUID();
        persistence.createJob(id, "REMOVED_TYPE", mapper.createObjectNode());
        assertThat(worker.process(id, 30)).isEqualTo(JobWorker.Outcome.ACK);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT failure_type FROM job_attempts WHERE job_id = ?", String.class, id))
                .isEqualTo("UNSUPPORTED_JOB_TYPE");
    }

    @Test
    void admittedSimulationExecutesAndMissingJobDoesNotCreateAnything() {
        UUID id = UUID.randomUUID();
        persistence.createJob(id, "SIMULATED_WORK", mapper.createObjectNode().put("durationMillis", 0).put("failFirstAttempts", 0));
        assertThat(worker.process(id, 30)).isEqualTo(JobWorker.Outcome.ACK);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("COMPLETED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts", Integer.class)).isEqualTo(1);
        assertThat(worker.process(UUID.randomUUID(), 30)).isEqualTo(JobWorker.Outcome.REJECT);
    }

    private UUID create() {
        UUID id = UUID.randomUUID();
        persistence.createJob(id, "ECHO", mapper.createObjectNode().put("message", "hello"));
        return id;
    }
}