package com.sahil.jobplatform.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class JobPersistenceIntegrationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    private static final Instant START = Instant.parse("2026-10-21T10:00:00Z");

    @Autowired private JobPersistenceService service;
    @Autowired private JobRepository jobs;
    @Autowired private JobPayloadRepository payloads;
    @Autowired private JobAttemptRepository attempts;
    @Autowired private JobResultRepository results;
    @Autowired private OutboxRepository outbox;
    @Autowired private IdempotencyRecordRepository idempotency;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private ObjectMapper mapper;
    @Autowired private PlatformTransactionManager transactions;
    @Autowired private Flyway flyway;

    @BeforeEach
    void clearDisposableTestDatabase() {
        jdbc.execute("""
                TRUNCATE job_platform.idempotency_keys, job_platform.outbox,
                         job_platform.job_results, job_platform.job_attempts,
                         job_platform.job_payloads, job_platform.jobs
                """);
    }

    @Test
    void allSixTablesAndApprovedIndexesExistAndHibernateValidatesTheirMappings() {
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("2");
        assertThat(flyway.validateWithResult().validationSuccessful).isTrue();
        assertThat(jdbc.queryForList("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'job_platform' AND table_name <> 'flyway_schema_history'
                """, String.class)).containsExactlyInAnyOrder(
                "jobs", "job_payloads", "job_attempts", "job_results", "idempotency_keys", "outbox");
        assertThat(jdbc.queryForList("""
                SELECT indexname FROM pg_indexes WHERE schemaname = 'job_platform'
                """, String.class)).contains("jobs_retry_idx", "jobs_lease_idx",
                "one_running_attempt_per_job", "outbox_unpublished_idx");
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = 'job_platform' AND table_name = 'jobs' AND column_name = 'payload_id'
                """, Integer.class)).isZero();
    }

    @Test
    void createsAndRetrievesACommittedQueuedJobPayloadAndOneOutboxEvent() throws Exception {
        UUID id = UUID.randomUUID();
        JsonNode input = mapper.readTree("""
                {"message":"hello 🌍", "nested":{"active":true,"missing":null},
                 "items":[3,1,2], "large":123456789012345678901234567890, "price":10.25}
                """);
        Instant before = databaseNow();

        JobDetails created = service.createJob(id, "ECHO", input);
        JobDetails stored = service.findJob(id).orElseThrow();

        assertThat(stored).isEqualTo(created);
        assertThat(stored.payload()).isEqualTo(input);
        assertThat(stored.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(stored.attemptCount()).isZero();
        assertThat(stored.cycleAttemptCount()).isZero();
        assertThat(stored.attemptId()).isNull();
        assertThat(stored.leaseExpiry()).isNull();
        assertThat(stored.nextRetryAt()).isNull();
        assertThat(stored.startedAt()).isNull();
        assertThat(stored.completedAt()).isNull();
        assertThat(stored.createdAt()).isBetween(before, databaseNow());
        assertThat(payloads.findByJobId(id).orElseThrow().getCreatedAt()).isEqualTo(stored.createdAt());
        assertThat(outbox.findByJobIdOrderByCreatedAtAsc(id)).singleElement().satisfies(event -> {
            assertThat(event.getJobId()).isEqualTo(id);
            assertThat(event.isPublished()).isFalse();
            assertThat(event.getPublishedAt()).isNull();
            assertThat(event.getCreatedAt()).isEqualTo(stored.createdAt());
        });
        assertThat(attempts.findByJobIdOrderByAttemptNumberAsc(id)).isEmpty();
        assertThat(results.findByJobId(id)).isEmpty();
        assertThat(count("idempotency_keys")).isZero();
    }

    @Test
    void payloadsAreDetachedDefensiveCopies() {
        UUID id = UUID.randomUUID();
        ObjectNode input = mapper.createObjectNode().put("message", "original");
        JobDetails created = service.createJob(id, "ECHO", input);
        input.put("message", "changed input");
        ((ObjectNode) created.payload()).put("message", "changed snapshot");

        assertThat(created.payload().path("message").asText()).isEqualTo("original");
        assertThat(service.findJob(id).orElseThrow().payload().path("message").asText())
                .isEqualTo("original");
    }

    @Test
    void missingJobReturnsEmptyRatherThanCreatingOrClaimingIt() {
        assertThat(service.findJob(UUID.randomUUID())).isEmpty();
        assertThat(count("jobs")).isZero();
        assertThat(count("outbox")).isZero();
    }

    @Test
    void duplicateJobIdCannotOverwriteOriginalPayloadOrCreateAnotherEvent() {
        UUID id = UUID.randomUUID();
        JobDetails original = service.createJob(id, "ECHO", mapper.createObjectNode().put("message", "first"));

        assertSqlState(catchThrowable(() -> service.createJob(id, "ECHO",
                mapper.createObjectNode().put("message", "second"))), "23505");

        assertThat(service.findJob(id)).contains(original);
        assertThat(count("jobs")).isEqualTo(1);
        assertThat(count("job_payloads")).isEqualTo(1);
        assertThat(count("outbox")).isEqualTo(1);
    }

    @Test
    void concurrentCreatesOfTheSameIdCommitOnlyOneJobPayloadAndEvent() throws Exception {
        UUID id = UUID.randomUUID();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var operation = (java.util.concurrent.Callable<Boolean>) () -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Concurrent test did not start");
                }
                Throwable failure = catchThrowable(() -> createJob(id));
                if (failure == null) {
                    return true;
                }
                assertSqlState(failure, "23505");
                return false;
            };
            var first = executor.submit(operation);
            var second = executor.submit(operation);
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(first.get(30, TimeUnit.SECONDS) ^ second.get(30, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(count("jobs")).isEqualTo(1);
        assertThat(count("job_payloads")).isEqualTo(1);
        assertThat(count("outbox")).isEqualTo(1);
        assertThat(service.findJob(id)).isPresent();
    }

    @Test
    void databaseFailureOnOutboxInsertRollsBackTheAlreadyFlushedJobAndPayload() {
        UUID id = UUID.randomUUID();
        jdbc.execute("ALTER TABLE outbox ADD CONSTRAINT test_reject_event CHECK (job_id <> '" + id + "'::uuid)");
        try {
            assertSqlState(catchThrowable(() -> createJob(id)), "23514");
        } finally {
            jdbc.execute("ALTER TABLE outbox DROP CONSTRAINT test_reject_event");
        }

        assertThat(jobs.findById(id)).isEmpty();
        assertThat(payloads.findByJobId(id)).isEmpty();
        assertThat(outbox.findByJobIdOrderByCreatedAtAsc(id)).isEmpty();
    }

    @Test
    void joinsAnOuterTransactionSoRequestOutcomeFailureCanRollBackAllNewWork() {
        var key = new IdempotencyRecordId("local-client", "SUBMIT_JOB", "same-key");
        insertOutcome(key, "a".repeat(64), "{\"jobId\":\"original\"}");
        UUID newJob = UUID.randomUUID();

        assertThatThrownBy(() -> new TransactionTemplate(transactions).executeWithoutResult(tx -> {
            createJob(newJob);
            insertOutcome(key, "b".repeat(64), "{\"jobId\":\"new\"}");
        })).isInstanceOf(DataIntegrityViolationException.class);

        assertThat(jobs.findById(newJob)).isEmpty();
        assertThat(payloads.findByJobId(newJob)).isEmpty();
        assertThat(outbox.findByJobIdOrderByCreatedAtAsc(newJob)).isEmpty();
        assertThat(idempotency.findById(key).orElseThrow().getRequestHash()).isEqualTo("a".repeat(64));
        assertThat(count("idempotency_keys")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"status = 'UNKNOWN'", "status = NULL", "type = NULL",
            "attempt_count = -1", "cycle_attempt_count = -1", "cycle_attempt_count = 5, attempt_count = 5",
            "cycle_attempt_count = 1, attempt_count = 0", "created_at = NULL"})
    void databaseRejectsInvalidJobStateAndCounters(String change) {
        UUID id = UUID.randomUUID();
        createJob(id);
        assertThatThrownBy(() -> jdbc.update("UPDATE jobs SET " + change + " WHERE job_id = ?", id))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(jobs.findById(id).orElseThrow().getStatus()).isEqualTo(JobStatus.QUEUED);
    }

    static Stream<Arguments> invalidAttempts() {
        UUID worker = UUID.randomUUID();
        return Stream.of(
                Arguments.of(0, worker, "RUNNING", START, "23514"),
                Arguments.of(-1, worker, "RUNNING", START, "23514"),
                Arguments.of(null, worker, "RUNNING", START, "23502"),
                Arguments.of(1, null, "RUNNING", START, "23502"),
                Arguments.of(1, worker, null, START, "23502"),
                Arguments.of(1, worker, "UNKNOWN", START, "23514"),
                Arguments.of(1, worker, "RUNNING", null, "23502"));
    }

    @ParameterizedTest
    @MethodSource("invalidAttempts")
    void databaseRejectsInvalidOrMissingAttemptFields(Integer number, UUID worker, String status,
                                                     Instant startedAt, String sqlState) {
        UUID id = UUID.randomUUID();
        createJob(id);
        assertSqlState(catchThrowable(() -> insertAttempt(id, number, worker, status, startedAt)), sqlState);
    }

    @Test
    void onlyOneRecordedRunningAttemptIsAllowedButExpiredHistoryDoesNotBlockTheNext() {
        UUID id = UUID.randomUUID();
        createJob(id);
        UUID first = insertAttempt(id, 1, UUID.randomUUID(), "RUNNING", START);

        assertSqlState(catchThrowable(() -> insertAttempt(id, 2, UUID.randomUUID(), "RUNNING", START)), "23505");
        jdbc.update("UPDATE job_attempts SET status = 'EXPIRED', ended_at = ? WHERE attempt_id = ?",
                timestamp(START.plusSeconds(30)), first);
        UUID second = insertAttempt(id, 2, UUID.randomUUID(), "RUNNING", START.plusSeconds(31));

        assertThat(attempts.findByJobIdOrderByAttemptNumberAsc(id)).extracting(JobAttempt::getAttemptNumber)
                .containsExactly(1, 2);
        assertThat(attempts.findById(first).orElseThrow().getStatus()).isEqualTo(AttemptStatus.EXPIRED);
        assertThat(attempts.findById(second).orElseThrow().getStatus()).isEqualTo(AttemptStatus.RUNNING);
        assertSqlState(catchThrowable(() -> insertAttempt(id, 1, UUID.randomUUID(), "FAILED", START)), "23505");
    }

    @Test
    void multipleNonRunningHistoricalAttemptsKeepTheirErrorDetailsAndTimes() {
        UUID id = UUID.randomUUID();
        createJob(id);
        UUID first = insertAttempt(id, 1, UUID.randomUUID(), "FAILED", START);
        insertAttempt(id, 2, UUID.randomUUID(), "EXPIRED", START.plusSeconds(2));
        jdbc.update("UPDATE job_attempts SET failure_type = ?, failure_reason = ?, ended_at = ? WHERE attempt_id = ?",
                "TIMEOUT", "temporary error", timestamp(START.plusSeconds(1)), first);

        JobAttempt history = attempts.findByJobIdOrderByAttemptNumberAsc(id).getFirst();
        assertThat(history.getFailureType()).isEqualTo("TIMEOUT");
        assertThat(history.getFailureReason()).isEqualTo("temporary error");
        assertThat(history.getStartedAt()).isEqualTo(START);
        assertThat(history.getEndedAt()).isEqualTo(START.plusSeconds(1));
    }

    @Test
    void payloadAndResultReferencesAreUniqueAndResultsRoundTripAsJson() throws Exception {
        UUID id = UUID.randomUUID();
        createJob(id);
        assertSqlState(catchThrowable(() -> jdbc.update("""
                INSERT INTO job_payloads (payload_id, job_id, payload) VALUES (?, ?, '{}'::jsonb)
                """, UUID.randomUUID(), id)), "23505");

        JsonNode result = mapper.readTree("{\"rows\":5,\"values\":[true,null,\"ok\"]}");
        jdbc.update("""
                INSERT INTO job_results (result_id, job_id, result, storage_type)
                VALUES (?, ?, ?::jsonb, 'DATABASE')
                """, UUID.randomUUID(), id, result.toString());
        JobResult stored = results.findByJobId(id).orElseThrow();
        assertThat(stored.getStorageType()).isEqualTo("DATABASE");
        assertThat(stored.getResult()).isEqualTo(result);
        assertThat(stored.getLocation()).isNull();
        assertSqlState(catchThrowable(() -> jdbc.update("""
                INSERT INTO job_results (result_id, job_id, storage_type) VALUES (?, ?, 'DATABASE')
                """, UUID.randomUUID(), id)), "23505");
    }

    @Test
    void externalResultReferenceCanBeReadWithoutImplementingObjectStorage() {
        UUID id = UUID.randomUUID();
        createJob(id);
        jdbc.update("""
                INSERT INTO job_results (result_id, job_id, storage_type, location)
                VALUES (?, ?, 'S3', 's3://example/result')
                """, UUID.randomUUID(), id);

        JobResult stored = results.findByJobId(id).orElseThrow();
        assertThat(stored.getResult()).isNull();
        assertThat(stored.getLocation()).isEqualTo("s3://example/result");
    }

    @ParameterizedTest
    @ValueSource(strings = {"payload", "attempt", "result", "outbox"})
    void dependentRowsCannotReferenceAMissingJob(String kind) {
        UUID missing = UUID.randomUUID();
        Throwable failure = catchThrowable(() -> {
            switch (kind) {
                case "payload" -> jdbc.update("""
                        INSERT INTO job_payloads (payload_id, job_id, payload) VALUES (?, ?, '{}'::jsonb)
                        """, UUID.randomUUID(), missing);
                case "attempt" -> insertAttempt(missing, 1, UUID.randomUUID(), "RUNNING", START);
                case "result" -> jdbc.update("""
                        INSERT INTO job_results (result_id, job_id, storage_type) VALUES (?, ?, 'DATABASE')
                        """, UUID.randomUUID(), missing);
                case "outbox" -> jdbc.update("INSERT INTO outbox (outbox_id, job_id) VALUES (?, ?)",
                        UUID.randomUUID(), missing);
                default -> throw new AssertionError(kind);
            }
        });
        assertSqlState(failure, "23503");
    }

    @Test
    void savedRequestOutcomesAreScopedAndReplayDataRoundTripsWithoutImplementingReplay() throws Exception {
        var first = new IdempotencyRecordId("local-client", "SUBMIT_JOB", "shared-key");
        var batch = new IdempotencyRecordId("local-client", "SUBMIT_BATCH", "shared-key");
        var otherClient = new IdempotencyRecordId("another-client", "SUBMIT_JOB", "shared-key");
        String response = "{\"acceptedCount\":1,\"items\":[{\"index\":0,\"jobId\":\"example\"}]}";
        insertOutcome(first, "a".repeat(64), response);
        insertOutcome(batch, "b".repeat(64), response);
        insertOutcome(otherClient, "c".repeat(64), response);

        IdempotencyRecord saved = idempotency.findById(first).orElseThrow();
        assertThat(saved.getResponseStatus()).isEqualTo(202);
        assertThat(saved.getResponseBody()).isEqualTo(mapper.readTree(response));
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(idempotency.findById(batch)).isPresent();
        assertThat(idempotency.findById(otherClient)).isPresent();
        assertSqlState(catchThrowable(() -> insertOutcome(first, "d".repeat(64), response)), "23505");
        assertThat(count("idempotency_keys")).isEqualTo(3);
    }

    @Test
    void multipleOutboxEventsPerJobAreAllowedAndPublishedEventsRemainStored() {
        UUID id = UUID.randomUUID();
        createJob(id);
        jdbc.update("INSERT INTO outbox (outbox_id, job_id) VALUES (?, ?)", UUID.randomUUID(), id);
        UUID first = outbox.findByJobIdOrderByCreatedAtAsc(id).getFirst().getOutboxId();
        jdbc.update("UPDATE outbox SET published = true, published_at = clock_timestamp() WHERE outbox_id = ?", first);

        assertThat(outbox.findByJobIdOrderByCreatedAtAsc(id)).hasSize(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published = false", Integer.class))
                .isEqualTo(1);
        assertThat(outbox.findByJobIdOrderByCreatedAtAsc(id).stream().filter(OutboxEvent::isPublished))
                .singleElement().satisfies(event -> assertThat(event.getPublishedAt()).isNotNull());
    }

    @Test
    void rawDatabaseDefaultsInitializeQueuedCountersAndAllowAnUnconstrainedAttemptPointer() {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO jobs (job_id, type, attempt_id) VALUES (?, 'ECHO', ?)", id, UUID.randomUUID());
        Job stored = jobs.findById(id).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(JobStatus.QUEUED);
        assertThat(stored.getAttemptCount()).isZero();
        assertThat(stored.getCycleAttemptCount()).isZero();
        assertThat(stored.getCreatedAt()).isNotNull();
        // A FK/UNIQUE on the payload enforces at most one, not at least one.
        assertThatThrownBy(() -> service.findJob(id)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has no payload");
    }

    @Test
    void rejectsMissingInputsBeforeWritingAnyRows() {
        assertThatThrownBy(() -> service.createJob(null, "ECHO", mapper.createObjectNode()))
                .isInstanceOf(NullPointerException.class);
        for (String type : new String[]{null, "", "   ", "x".repeat(256)}) {
            assertThatThrownBy(() -> service.createJob(UUID.randomUUID(), type, mapper.createObjectNode()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (JsonNode payload : new JsonNode[]{null, mapper.nullNode(), mapper.missingNode()}) {
            assertThatThrownBy(() -> service.createJob(UUID.randomUUID(), "ECHO", payload))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(count("jobs")).isZero();
        assertThat(count("job_payloads")).isZero();
        assertThat(count("outbox")).isZero();
    }

    private void createJob(UUID id) {
        service.createJob(id, "ECHO", mapper.createObjectNode().put("message", "hello"));
    }

    private UUID insertAttempt(UUID jobId, Integer number, UUID worker, String status, Instant startedAt) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO job_attempts (attempt_id, job_id, attempt_number, worker_id, status, started_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """, id, jobId, number, worker, status, timestamp(startedAt));
        return id;
    }

    private void insertOutcome(IdempotencyRecordId id, String hash, String response) {
        jdbc.update("""
                INSERT INTO idempotency_keys
                    (client_id, operation_scope, idempotency_key, request_hash, response_status, response_body)
                VALUES (?, ?, ?, ?, 202, ?::jsonb)
                """, id.getClientId(), id.getOperationScope(), id.getIdempotencyKey(), hash, response);
    }

    private int count(String table) {
        return jdbc.queryForObject("SELECT count(*) FROM job_platform." + table, Integer.class);
    }

    private Instant databaseNow() {
        return jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class).toInstant();
    }

    private static OffsetDateTime timestamp(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    private static void assertSqlState(Throwable failure, String expected) {
        assertThat(failure).isNotNull();
        Throwable cause = failure;
        while (!(cause instanceof SQLException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertThat(cause).isInstanceOf(SQLException.class);
        assertThat(((SQLException) cause).getSQLState()).isEqualTo(expected);
    }
}