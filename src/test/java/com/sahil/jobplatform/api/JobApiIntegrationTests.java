package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class JobApiIntegrationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    private static final String ECHO = "{\"type\":\"ECHO\",\"payload\":{\"message\":\"hello\"}}";

    @Autowired private TestRestTemplate http;
    @Autowired private ObjectMapper mapper;
    @Autowired private JdbcTemplate jdbc;

    @BeforeEach
    void cleanDisposableDatabase() {
        jdbc.execute("""
                TRUNCATE job_platform.idempotency_keys, job_platform.outbox,
                job_platform.job_results, job_platform.job_attempts, job_platform.job_payloads, job_platform.jobs
                """);
    }

    @Test
    void submitCommitsEveryAcceptanceRecordAndRetrievesQueuedJob() throws Exception {
        var response = post("/jobs", "new-job", ECHO);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        JsonNode accepted = body(response);
        UUID id = UUID.fromString(accepted.path("jobId").asText());
        assertThat(accepted.path("status").asText()).isEqualTo("QUEUED");
        assertThat(response.getHeaders().getLocation().getPath()).isEqualTo("/jobs/" + id);
        assertAcceptanceCounts(1, 1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM idempotency_keys WHERE response_status = 0", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject("SELECT client_id FROM idempotency_keys", String.class)).isEqualTo("local-client");
        assertThat(jdbc.queryForObject("SELECT operation_scope FROM idempotency_keys", String.class)).isEqualTo("SUBMIT_JOB");

        var retrieved = http.getForEntity("/jobs/" + id, String.class);
        assertThat(retrieved.getStatusCode().value()).isEqualTo(200);
        JsonNode job = body(retrieved);
        assertThat(job.path("payload")).isEqualTo(mapper.readTree(ECHO).get("payload"));
        assertThat(job.path("status").asText()).isEqualTo("QUEUED");
        assertThat(job.path("attemptCount").asInt()).isZero();
        assertThat(job.path("cycleAttemptCount").asInt()).isZero();
        assertThat(job.path("createdAt").isTextual()).isTrue();
        for (String field : List.of("startedAt", "completedAt", "attemptId", "leaseExpiry", "failure", "result")) {
            assertThat(job.path(field).isNull()).as(field).isTrue();
        }
        assertThat(count("job_attempts")).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published", Integer.class)).isZero();
    }

    @Test
    void replayIgnoresWhitespaceObjectOrderAndReturnsOriginalStatusAfterJobChanges() throws Exception {
        var first = post("/jobs", "replay", ECHO);
        UUID id = UUID.fromString(body(first).path("jobId").asText());
        jdbc.update("UPDATE jobs SET status = 'COMPLETED', completed_at = clock_timestamp() WHERE job_id = ?", id);

        var repeated = post("/jobs", "replay", " { \"payload\" : {\"message\":\"hello\"}, \"type\" : \"ECHO\" } ");
        assertThat(repeated.getStatusCode().value()).isEqualTo(202);
        assertThat(body(repeated)).isEqualTo(body(first));
        assertThat(repeated.getHeaders().getLocation()).isEqualTo(first.getHeaders().getLocation());
        assertThat(body(http.getForEntity("/jobs/" + id, String.class)).path("status").asText()).isEqualTo("COMPLETED");
        assertAcceptanceCounts(1, 1);
    }

    @Test
    void changedInputReturnsConflictAndLeavesOriginalOutcomeUntouched() throws Exception {
        var original = post("/jobs", "reuse", ECHO);
        var changed = post("/jobs", "reuse", ECHO.replace("hello", "different"));
        assertError(changed, 409, "IDEMPOTENCY_KEY_REUSED");
        assertThat(body(post("/jobs", "reuse", ECHO))).isEqualTo(body(original));
        assertAcceptanceCounts(1, 1);
    }

    @Test
    void concurrentMatchingRequestsCommitOnlyOneAcceptanceAndAllReplaySameJob() throws Exception {
        var responses = concurrently(
                () -> post("/jobs", "concurrent", ECHO),
                () -> post("/jobs", "concurrent", ECHO),
                () -> post("/jobs", "concurrent", ECHO),
                () -> post("/jobs", "concurrent", ECHO));
        for (var response : responses) {
            assertThat(response.getStatusCode().value()).isEqualTo(202);
            assertThat(body(response)).isEqualTo(body(responses.getFirst()));
        }
        assertAcceptanceCounts(1, 1);
    }

    @Test
    void concurrentDifferentInputsHaveOneWinnerAndOneConflict() throws Exception {
        var responses = concurrently(() -> post("/jobs", "race", ECHO),
                () -> post("/jobs", "race", ECHO.replace("hello", "different")));
        assertThat(responses.stream().map(r -> r.getStatusCode().value())).containsExactlyInAnyOrder(202, 409);
        assertAcceptanceCounts(1, 1);
    }

    @Test
    void batchAcceptsValidItemsAndReplaysOriginalPerItemOutcomes() throws Exception {
        String batch = "{\"items\":[" + ECHO + ",{\"type\":\"UNKNOWN\",\"payload\":{}},"
                + "{\"type\":\"SIMULATED_WORK\",\"payload\":{\"durationMillis\":1000,\"failFirstAttempts\":2}}]}";
        var first = post("/jobs/batch", "batch", batch);
        assertThat(first.getStatusCode().value()).isEqualTo(202);
        JsonNode result = body(first);
        assertThat(result.path("acceptedCount").asInt()).isEqualTo(2);
        assertThat(result.path("rejectedCount").asInt()).isEqualTo(1);
        assertThat(result.path("items").size()).isEqualTo(3);
        for (int i = 0; i < 3; i++) { assertThat(result.path("items").get(i).path("index").asInt()).isEqualTo(i); }
        assertThat(result.path("items").get(1).path("error").path("code").asText()).isEqualTo("UNSUPPORTED_JOB_TYPE");
        assertThat(result.path("items").get(1).has("jobId")).isFalse();
        assertThat(body(post("/jobs/batch", "batch", batch))).isEqualTo(result);
        assertAcceptanceCounts(2, 1);
    }

    @Test
    void allInvalidBatchSavesAndReplays400WithoutJobsAndChangedInvalidItemConflicts() throws Exception {
        String input = "{\"items\":[{},null,5,{\"type\":\"ECHO\",\"payload\":null}]}";
        var original = post("/jobs/batch", "all-invalid", input);
        assertThat(original.getStatusCode().value()).isEqualTo(400);
        assertThat(body(original).path("acceptedCount").asInt()).isZero();
        assertThat(body(original).path("rejectedCount").asInt()).isEqualTo(4);
        var replay = post("/jobs/batch", "all-invalid", input);
        assertThat(replay.getStatusCode().value()).isEqualTo(400);
        assertThat(body(replay)).isEqualTo(body(original));
        assertError(post("/jobs/batch", "all-invalid", input.replace("5", "6")), 409, "IDEMPOTENCY_KEY_REUSED");
        assertAcceptanceCounts(0, 1);
    }

    @Test
    void batchOrderMattersAndScopesAllowSameKeyForSingleAndBatch() throws Exception {
        post("/jobs", "shared", ECHO);
        String first = "{\"items\":[" + ECHO + ",{}]}";
        assertThat(post("/jobs/batch", "shared", first).getStatusCode().value()).isEqualTo(202);
        assertError(post("/jobs/batch", "shared", "{\"items\":[{}," + ECHO + "]}"), 409, "IDEMPOTENCY_KEY_REUSED");
        assertAcceptanceCounts(2, 2);
    }

    @Test
    void concurrentBatchReplaysHaveIdenticalIdsAndRejectedItems() throws Exception {
        String batch = "{\"items\":[" + ECHO + ",{}," + ECHO + "]}";
        var responses = concurrently(() -> post("/jobs/batch", "batch-race", batch),
                () -> post("/jobs/batch", "batch-race", batch));
        assertThat(responses).allSatisfy(response -> assertThat(response.getStatusCode().value()).isEqualTo(202));
        assertThat(body(responses.get(0))).isEqualTo(body(responses.get(1)));
        assertAcceptanceCounts(2, 1);
    }

    @Test
    void databaseFailureRollsBackJobsPayloadsEventsAndTheReservedKey() throws Exception {
        jdbc.execute("ALTER TABLE outbox ADD CONSTRAINT test_api_reject CHECK (published = true)");
        try {
            var failure = post("/jobs", "retry-after-storage-failure", ECHO);
            assertError(failure, 503, "STORAGE_UNAVAILABLE");
            assertThat(failure.getBody()).doesNotContain("INSERT", "jdbc:", "constraint", "test_api_reject");
            assertAcceptanceCounts(0, 0);
        } finally {
            jdbc.execute("ALTER TABLE outbox DROP CONSTRAINT test_api_reject");
        }
        assertThat(post("/jobs", "retry-after-storage-failure", ECHO).getStatusCode().value()).isEqualTo(202);
        assertAcceptanceCounts(1, 1);
    }

    @Test
    void laterBatchInsertFailureRollsBackEvenEarlierValidItemsAndSavedOutcome() throws Exception {
        jdbc.execute("ALTER TABLE jobs ADD CONSTRAINT test_api_reject_simulation CHECK (type <> 'SIMULATED_WORK')");
        String input = "{\"items\":[" + ECHO + ","
                + "{\"type\":\"SIMULATED_WORK\",\"payload\":{\"durationMillis\":1,\"failFirstAttempts\":0}}]}";
        try {
            assertError(post("/jobs/batch", "rollback-batch", input), 503, "STORAGE_UNAVAILABLE");
            assertAcceptanceCounts(0, 0);
        } finally {
            jdbc.execute("ALTER TABLE jobs DROP CONSTRAINT test_api_reject_simulation");
        }
        assertThat(post("/jobs/batch", "rollback-batch", input).getStatusCode().value()).isEqualTo(202);
        assertAcceptanceCounts(2, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void blankKeysRejectedBeforeAcceptance(String key) throws Exception {
        assertError(post("/jobs", key, ECHO), 400, "INVALID_IDEMPOTENCY_KEY");
        assertAcceptanceCounts(0, 0);
    }

    @Test
    void missingDuplicateAndTooLongKeysAreRejected() throws Exception {
        assertError(post("/jobs", null, ECHO), 400, "INVALID_IDEMPOTENCY_KEY");
        assertError(post("/jobs", "x".repeat(201), ECHO), 400, "INVALID_IDEMPOTENCY_KEY");
        HttpHeaders headers = jsonHeaders();
        headers.add("Idempotency-Key", "first");
        headers.add("Idempotency-Key", "second");
        assertError(http.postForEntity("/jobs", new HttpEntity<>(ECHO, headers), String.class),
                400, "INVALID_IDEMPOTENCY_KEY");
        assertAcceptanceCounts(0, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{", "", "{\"type\":\"ECHO\",\"type\":\"OTHER\",\"payload\":{}}",
            "{\"type\":\"ECHO\",\"payload\":{\"n\":1,\"n\":2}}", "{} {}", "NaN"})
    void malformedAmbiguousAndTrailingJsonIsRejectedWithoutStoredOutcome(String input) throws Exception {
        assertError(post("/jobs", "bad-json", input), 400, "INVALID_JSON");
        assertAcceptanceCounts(0, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"items\":[]}", "{\"items\":{}}", "{\"items\":null}",
            "{\"items\":[{}],\"extra\":true}"})
    void invalidBatchEnvelopeIsNotPartiallyAcceptedOrRecorded(String input) throws Exception {
        assertError(post("/jobs/batch", "bad-envelope", input), 400, "INVALID_BATCH");
        assertAcceptanceCounts(0, 0);
    }

    @Test
    void batchLimitRejects101ItemsWithoutInserts() throws Exception {
        var batch = mapper.createObjectNode();
        var items = batch.putArray("items");
        for (int i = 0; i < 101; i++) { items.add(mapper.readTree(ECHO)); }
        assertError(post("/jobs/batch", "too-large", batch.toString()), 400, "INVALID_BATCH");
        assertAcceptanceCounts(0, 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"type\":1,\"payload\":{}}", "{\"type\":\" \"}",
            "{\"type\":\"UNKNOWN\",\"payload\":{}}", "{\"type\":\"ECHO\"}",
            "{\"type\":\"ECHO\",\"payload\":null}", "{\"type\":\"ECHO\",\"payload\":{},\"extra\":1}",
            "{\"type\":\"SIMULATED_WORK\",\"payload\":{}}",
            "{\"type\":\"SIMULATED_WORK\",\"payload\":{\"durationMillis\":-1,\"failFirstAttempts\":0}}",
            "{\"type\":\"SIMULATED_WORK\",\"payload\":{\"durationMillis\":1.5,\"failFirstAttempts\":0}}",
            "{\"type\":\"SIMULATED_WORK\",\"payload\":{\"durationMillis\":1,\"failFirstAttempts\":2147483648}}"})
    void invalidSingleRequestSavesAReplayable400ButNoJob(String input) throws Exception {
        var initial = post("/jobs", "invalid-item", input);
        assertThat(initial.getStatusCode().value()).isEqualTo(400);
        var replay = post("/jobs", "invalid-item", input);
        assertThat(body(replay)).isEqualTo(body(initial));
        assertError(post("/jobs", "invalid-item", ECHO), 409, "IDEMPOTENCY_KEY_REUSED");
        assertAcceptanceCounts(0, 1);
    }

    @Test
    void preciseDecimalPayloadRoundTripsAndTinyNumericChangesConflict() throws Exception {
        String decimal = "0.123456789012345678901234567890123456789";
        String input = "{\"type\":\"ECHO\",\"payload\":{\"value\":" + decimal + "}}";
        var response = post("/jobs", "precise", input);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        var stored = body(http.getForEntity("/jobs/" + body(response).path("jobId").asText(), String.class));
        assertThat(stored.path("payload").path("value").decimalValue()).isEqualByComparingTo(decimal);
        assertError(post("/jobs", "precise", input.replace(decimal, decimal + "1")), 409, "IDEMPOTENCY_KEY_REUSED");
        assertAcceptanceCounts(1, 1);
    }

    @Test
    void equivalentIntegerNumericSpellingsValidateAndReplayConsistently() throws Exception {
        String input = "{\"type\":\"SIMULATED_WORK\",\"payload\":{\"durationMillis\":1.0,\"failFirstAttempts\":0e0}}";
        var response = post("/jobs", "whole-numbers", input);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(body(post("/jobs", "whole-numbers", input.replace("1.0", "1").replace("0e0", "0"))))
                .isEqualTo(body(response));
        assertAcceptanceCounts(1, 1);
    }

    @Test
    void filtersAndBoundsListsWithoutLoadingPayloads() throws Exception {
        post("/jobs", "one", ECHO);
        var second = body(post("/jobs", "two", ECHO));
        jdbc.update("UPDATE jobs SET status = 'FAILED' WHERE job_id = ?", UUID.fromString(second.path("jobId").asText()));
        var listed = body(http.getForEntity("/jobs?status=FAILED&limit=1", String.class));
        assertThat(listed.path("jobs").size()).isEqualTo(1);
        assertThat(listed.path("jobs").get(0).path("jobId")).isEqualTo(second.path("jobId"));
        assertThat(listed.path("jobs").get(0).has("payload")).isFalse();
        assertThat(body(http.getForEntity("/jobs?limit=1", String.class)).path("jobs").size()).isEqualTo(1);
        assertError(http.getForEntity("/jobs?status=INVALID", String.class), 400, "INVALID_PARAMETER");
        assertError(http.getForEntity("/jobs?limit=0", String.class), 400, "INVALID_LIMIT");
        assertError(http.getForEntity("/jobs?limit=101", String.class), 400, "INVALID_LIMIT");
        assertError(http.getForEntity("/jobs?limit=x", String.class), 400, "INVALID_PARAMETER");
    }

    @Test
    void missingAndMalformedIdsReturnDifferentStructuredErrors() throws Exception {
        assertError(http.getForEntity("/jobs/" + UUID.randomUUID(), String.class), 404, "JOB_NOT_FOUND");
        assertError(http.getForEntity("/jobs/not-a-uuid", String.class), 400, "INVALID_PARAMETER");
    }

    @Test
    void retrievalUsesCurrentOutcomeWithoutLeakingOldFailureIntoNewCycleOrSuccess() throws Exception {
        UUID id = UUID.fromString(body(post("/jobs", "history", ECHO)).path("jobId").asText());
        UUID attempt = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO job_attempts (attempt_id, job_id, attempt_number, worker_id, status,
                    started_at, ended_at, failure_type, failure_reason)
                VALUES (?, ?, 1, ?, 'FAILED', clock_timestamp(), clock_timestamp(), 'TIMEOUT', 'old timeout')
                """, attempt, id, UUID.randomUUID());
        jdbc.update("UPDATE jobs SET status = 'FAILED', attempt_count = 1, cycle_attempt_count = 1, attempt_id = ? WHERE job_id = ?",
                attempt, id);
        assertThat(body(http.getForEntity("/jobs/" + id, String.class)).path("failure").path("type").asText())
                .isEqualTo("TIMEOUT");
        jdbc.update("UPDATE jobs SET status = 'RETRYING' WHERE job_id = ?", id);
        assertThat(body(http.getForEntity("/jobs/" + id, String.class)).path("failure").path("type").asText())
            .isEqualTo("TIMEOUT");
        jdbc.update("UPDATE jobs SET status = 'QUEUED', cycle_attempt_count = 0 WHERE job_id = ?", id);
        assertThat(body(http.getForEntity("/jobs/" + id, String.class)).path("failure").isNull()).isTrue();
        jdbc.update("UPDATE jobs SET status = 'RUNNING', cycle_attempt_count = 1 WHERE job_id = ?", id);
        assertThat(body(http.getForEntity("/jobs/" + id, String.class)).path("failure").isNull()).isTrue();
        jdbc.update("UPDATE jobs SET status = 'COMPLETED' WHERE job_id = ?", id);
        jdbc.update("INSERT INTO job_results (result_id, job_id, result, storage_type) VALUES (?, ?, '{\"ok\":true}'::jsonb, 'DATABASE')",
                UUID.randomUUID(), id);
        var completed = body(http.getForEntity("/jobs/" + id, String.class));
        assertThat(completed.path("failure").isNull()).isTrue();
        assertThat(completed.path("result").path("value").path("ok").asBoolean()).isTrue();
    }

    @Test
    void nonJsonContentTypeAndRetryWithoutAKeyAreNotSilentlyAccepted() throws Exception {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.set("Idempotency-Key", "wrong-media");
        assertError(http.postForEntity("/jobs", new HttpEntity<>(ECHO, headers), String.class),
            415, "UNSUPPORTED_MEDIA_TYPE");
        assertThat(http.exchange("/jobs/" + UUID.randomUUID() + "/retry", HttpMethod.POST,
                new HttpEntity<>("{}", jsonHeaders()), String.class).getStatusCode().value()).isEqualTo(400);
        assertAcceptanceCounts(0, 0);
    }

    private ResponseEntity<String> post(String path, String key, String json) {
        HttpHeaders headers = jsonHeaders();
        if (key != null) { headers.add("Idempotency-Key", key); }
        return http.postForEntity(path, new HttpEntity<>(json, headers), String.class);
    }

    private HttpHeaders jsonHeaders() {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    private JsonNode body(ResponseEntity<String> response) throws Exception {
        return mapper.readTree(response.getBody());
    }

    private void assertError(ResponseEntity<String> response, int status, String code) throws Exception {
        assertThat(response.getStatusCode().value()).isEqualTo(status);
        assertThat(body(response).path("error").path("code").asText()).isEqualTo(code);
    }

    private void assertAcceptanceCounts(int jobs, int outcomes) {
        assertThat(count("jobs")).isEqualTo(jobs);
        assertThat(count("job_payloads")).isEqualTo(jobs);
        assertThat(count("outbox")).isEqualTo(jobs);
        assertThat(count("idempotency_keys")).isEqualTo(outcomes);
    }

    private int count(String table) {
        return jdbc.queryForObject("SELECT count(*) FROM job_platform." + table, Integer.class);
    }

    @SafeVarargs
    private final List<ResponseEntity<String>> concurrently(Callable<ResponseEntity<String>>... operations) throws Exception {
        var ready = new CountDownLatch(operations.length);
        var start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(operations.length)) {
            var futures = new ArrayList<java.util.concurrent.Future<ResponseEntity<String>>>();
            for (var operation : operations) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) { throw new IllegalStateException("Test start timed out"); }
                    return operation.call();
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            var results = new ArrayList<ResponseEntity<String>>();
            for (var future : futures) { results.add(future.get(40, TimeUnit.SECONDS)); }
            return results;
        }
    }
}