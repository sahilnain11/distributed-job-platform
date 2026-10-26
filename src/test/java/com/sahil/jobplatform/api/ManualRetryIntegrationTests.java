package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sahil.jobplatform.messaging.JobExecutionStore;
import com.sahil.jobplatform.persistence.JobPersistenceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ManualRetryIntegrationTests {

    @Container @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    @Autowired private TestRestTemplate http;
    @Autowired private ObjectMapper mapper;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private JobPersistenceService persistence;
    @Autowired private JobExecutionStore store;

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE idempotency_keys, outbox, job_results, job_attempts, job_payloads, jobs");
    }

    @Test
    void manualRetryPreservesLifetimeHistoryAndGrantsFreshBudgetWithoutCreatingAnAttempt() throws Exception {
        UUID id = failed();
        var before = jdbc.queryForObject("SELECT created_at FROM jobs WHERE job_id = ?", java.time.OffsetDateTime.class, id);
        var response = retry(id, "manual-a", null);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(mapper.readTree(response.getBody()).path("jobId").asText()).isEqualTo(id.toString());
        assertThat(jdbc.queryForMap("SELECT status, attempt_count, cycle_attempt_count FROM jobs WHERE job_id = ?", id))
                .containsEntry("status", "QUEUED").containsEntry("attempt_count", 4).containsEntry("cycle_attempt_count", 0);
        assertThat(jdbc.queryForObject("SELECT started_at IS NULL AND completed_at IS NULL AND next_retry_at IS NULL AND lease_expiry IS NULL FROM jobs WHERE job_id = ?",
                Boolean.class, id)).isTrue();
        assertThat(jdbc.queryForObject("SELECT created_at FROM jobs WHERE job_id = ?", java.time.OffsetDateTime.class, id)).isEqualTo(before);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts WHERE job_id = ?", Integer.class, id)).isEqualTo(4);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, id)).isEqualTo(2);
        var claim = store.claim(id, UUID.randomUUID(), 30);
        assertThat(claim.cycleAttempt()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT attempt_number FROM job_attempts WHERE attempt_id = ?", Integer.class, claim.attemptId()))
                .isEqualTo(5);
        assertThat(jdbc.queryForObject("SELECT operation_scope FROM idempotency_keys", String.class)).isEqualTo("RETRY_JOB:" + id);
    }

    @Test
    void oldKeyReplaysBeforeEligibilityEvenAfterASecondFailureAndNewKeyStartsAnotherCycle() throws Exception {
        UUID id = failed();
        var first = retry(id, "old-key", "{}");
        var attempt = store.claim(id, UUID.randomUUID(), 30);
        store.failPermanently(id, attempt.attemptId(), "PERMANENT", "another failure");
        var replay = retry(id, "old-key", null);
        assertThat(replay.getStatusCode().value()).isEqualTo(202);
        assertThat(mapper.readTree(replay.getBody())).isEqualTo(mapper.readTree(first.getBody()));
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("FAILED");
        assertThat(retry(id, "fresh-key", "{}").getStatusCode().value()).isEqualTo(202);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, id)).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM jobs WHERE job_id = ?", Integer.class, id)).isEqualTo(5);
    }

    @ParameterizedTest
    @ValueSource(strings = {"QUEUED", "RUNNING", "RETRYING", "COMPLETED"})
    void newRequestForNonFailedJobConflictsWithoutResetOrEvent(String status) throws Exception {
        UUID id = failed();
        jdbc.update("UPDATE jobs SET status = ? WHERE job_id = ?", status, id);
        var result = retry(id, "wrong-state", "{}");
        assertThat(result.getStatusCode().value()).isEqualTo(409);
        assertThat(mapper.readTree(result.getBody()).path("error").path("code").asText()).isEqualTo("JOB_NOT_FAILED");
        assertThat(jdbc.queryForObject("SELECT cycle_attempt_count FROM jobs WHERE job_id = ?", Integer.class, id)).isEqualTo(4);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
    }

    @Test
    void differentKeyRaceAcceptsOnceAndMatchingKeyRaceReplaysOneOutcome() throws Exception {
        UUID first = failed();
        var different = concurrent(first, "one", "two");
        assertThat(different.stream().map(response -> response.getStatusCode().value())).containsExactlyInAnyOrder(202, 409);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, first)).isEqualTo(2);
        UUID second = failed();
        var matching = concurrent(second, "same", "same");
        assertThat(matching.stream().map(response -> response.getStatusCode().value())).containsExactly(202, 202);
        assertThat(mapper.readTree(matching.get(0).getBody())).isEqualTo(mapper.readTree(matching.get(1).getBody()));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, second)).isEqualTo(2);
    }

    @Test
    void outboxFailureRollsBackManualResetAndRequestReservationThenSameKeyCanSucceed() {
        UUID id = failed();
        jdbc.execute("ALTER TABLE outbox ADD CONSTRAINT test_manual_event CHECK (job_id <> '" + id + "'::uuid) NOT VALID");
        try { assertThat(retry(id, "rollback", "{}").getStatusCode().value()).isEqualTo(503); }
        finally { jdbc.execute("ALTER TABLE outbox DROP CONSTRAINT test_manual_event"); }
        assertThat(jdbc.queryForMap("SELECT status, cycle_attempt_count FROM jobs WHERE job_id = ?", id))
                .containsEntry("status", "FAILED").containsEntry("cycle_attempt_count", 4);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM idempotency_keys", Integer.class)).isZero();
        assertThat(retry(id, "rollback", "{}").getStatusCode().value()).isEqualTo(202);
    }

    @Test
    void scopeIncludesCanonicalTargetAndChangedInputUnderSavedKeyConflicts() throws Exception {
        UUID first = failed(); UUID second = failed();
        assertThat(retry(first, "shared", "{}").getStatusCode().value()).isEqualTo(202);
        assertThat(retry(second, "shared", "{}").getStatusCode().value()).isEqualTo(202);
        var changed = retry(first, "shared", "{\"unexpected\":true}");
        assertThat(changed.getStatusCode().value()).isEqualTo(409);
        assertThat(mapper.readTree(changed.getBody()).path("error").path("code").asText()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        var headers = headers("shared");
        var upper = http.postForEntity("/jobs/" + first.toString().toUpperCase() + "/retry", new HttpEntity<>("{}", headers), String.class);
        assertThat(upper.getStatusCode().value()).isEqualTo(202);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM idempotency_keys", Integer.class)).isEqualTo(2);
    }

    @Test
    void missingInvalidAndBadInputRetriesDoNotCreateWork() {
        assertThat(retry(UUID.randomUUID(), "missing", "{}").getStatusCode().value()).isEqualTo(404);
        assertThat(http.postForEntity("/jobs/not-a-uuid/retry", new HttpEntity<>("{}", headers("bad-id")), String.class)
                .getStatusCode().value()).isEqualTo(400);
        UUID id = failed();
        assertThat(retry(id, "bad-body", "{\"extra\":1}").getStatusCode().value()).isEqualTo(400);
        assertThat(retry(id, null, "{}").getStatusCode().value()).isEqualTo(400);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("FAILED");
    }

    private UUID failed() {
        UUID id = UUID.randomUUID();
        persistence.createJob(id, "ECHO", mapper.createObjectNode().put("message", "manual retry"));
        for (int number = 1; number <= 4; number++) {
            UUID attempt = UUID.randomUUID();
            jdbc.update("""
                    INSERT INTO job_attempts (attempt_id, job_id, attempt_number, worker_id, status,
                        started_at, ended_at, failure_type, failure_reason)
                    VALUES (?, ?, ?, ?, 'FAILED', clock_timestamp(), clock_timestamp(), 'TEMPORARY', 'previous failure')
                    """, attempt, id, number, UUID.randomUUID());
            jdbc.update("UPDATE jobs SET attempt_id = ? WHERE job_id = ?", attempt, id);
        }
        jdbc.update("""
                UPDATE jobs SET status='FAILED', attempt_count=4, cycle_attempt_count=4,
                    started_at=clock_timestamp(), completed_at=clock_timestamp() WHERE job_id=?
                """, id);
        return id;
    }

    private ResponseEntity<String> retry(UUID id, String key, String body) {
        return http.postForEntity("/jobs/" + id + "/retry", new HttpEntity<>(body, headers(key)), String.class);
    }

    private HttpHeaders headers(String key) {
        var headers = new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON);
        if (key != null) { headers.set("Idempotency-Key", key); }
        return headers;
    }

    private List<ResponseEntity<String>> concurrent(UUID id, String keyA, String keyB) throws Exception {
        var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(2)) {
            java.util.function.Function<String, java.util.concurrent.Callable<ResponseEntity<String>>> task = key -> () -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) { throw new IllegalStateException("Test start timed out"); }
                return retry(id, key, "{}");
            };
            var a = pool.submit(task.apply(keyA)); var b = pool.submit(task.apply(keyB));
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
            return List.of(a.get(30, TimeUnit.SECONDS), b.get(30, TimeUnit.SECONDS));
        }
    }
}