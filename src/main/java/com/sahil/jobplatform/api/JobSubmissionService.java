package com.sahil.jobplatform.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sahil.jobplatform.persistence.JobPersistenceService;
import com.sahil.jobplatform.observability.JobTelemetry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Service
public class JobSubmissionService {

    private static final String CLIENT = "local-client";
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final RequestFingerprint fingerprints;
    private final SubmissionValidator validator;
    private final JobPersistenceService persistence;
    private final TransactionTemplate transaction;
    private final JobTelemetry telemetry;

    public JobSubmissionService(JdbcTemplate jdbc, ObjectMapper mapper, RequestFingerprint fingerprints,
                                SubmissionValidator validator, JobPersistenceService persistence,
                                PlatformTransactionManager transactions, JobTelemetry telemetry) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.fingerprints = fingerprints;
        this.validator = validator;
        this.persistence = persistence;
        this.telemetry = telemetry;
        this.transaction = new TransactionTemplate(transactions);
        this.transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.transaction.setTimeout(30);
    }

    public SubmissionOutcome submit(String key, JsonNode request, boolean batch) {
        ApiJson.require(request != null && request.isObject(), "INVALID_REQUEST", "Request must be a JSON object");
        if (batch) {
            ApiJson.require(ApiJson.onlyFields(request, "items") && request.path("items").isArray()
                            && !request.path("items").isEmpty() && request.path("items").size() <= 100,
                    "INVALID_BATCH", "Batch must contain only an items array with 1 to 100 jobs");
        }
        JsonNode input = request.deepCopy();
        String scope = batch ? "SUBMIT_BATCH" : "SUBMIT_JOB";
        return execute(key, scope, input, () -> batch ? createBatch(input) : createSingle(input));
    }

    public SubmissionOutcome retry(String key, UUID jobId, JsonNode request) {
        JsonNode input = request == null ? mapper.createObjectNode() : request.deepCopy();
        ApiJson.require(input.isObject(), "INVALID_REQUEST", "Retry body must be absent or an empty object");
        return execute(key, "RETRY_JOB:" + jobId, input, () -> {
            if (!input.isEmpty()) {
                return new SubmissionOutcome(400, ApiJson.error("INVALID_RETRY", "Retry has no input fields"));
            }
            var rows = jdbc.query("SELECT status FROM job_platform.jobs WHERE job_id = ? FOR UPDATE",
                    (row, number) -> row.getString(1), jobId);
            if (rows.isEmpty()) {
                return new SubmissionOutcome(404, ApiJson.error("JOB_NOT_FOUND", "Job does not exist"));
            }
            if (!"FAILED".equals(rows.getFirst())) {
                return new SubmissionOutcome(409, ApiJson.error("JOB_NOT_FAILED", "Job is not currently failed"));
            }
            int changed = jdbc.update("""
                    UPDATE job_platform.jobs SET status = 'QUEUED', cycle_attempt_count = 0,
                        started_at = NULL, completed_at = NULL, lease_expiry = NULL, next_retry_at = NULL
                    WHERE job_id = ? AND status = 'FAILED'
                    """, jobId);
            if (changed != 1) { throw new IllegalStateException("Manual retry transition was not accepted"); }
            jdbc.update("INSERT INTO job_platform.outbox (outbox_id, job_id) VALUES (?, ?)", UUID.randomUUID(), jobId);
            telemetry.retry(jobId, true);
            return new SubmissionOutcome(202, ApiJson.accepted(jobId));
        });
    }

    private SubmissionOutcome execute(String key, String scope, JsonNode input,
                                       java.util.function.Supplier<SubmissionOutcome> operation) {
        ApiJson.require(key != null && !key.isBlank() && key.length() <= 200,
                "INVALID_IDEMPOTENCY_KEY", "A nonblank Idempotency-Key of at most 200 characters is required");
        String hash = fingerprints.hash(input);

        java.util.concurrent.atomic.AtomicBoolean replayed = new java.util.concurrent.atomic.AtomicBoolean();
        SubmissionOutcome result = Objects.requireNonNull(transaction.execute(tx -> {
            // The unique insert waits for a competing request's transaction.
            // A placeholder is uncommitted/private and is always finalized below
            // or rolled back; it is never a saved API response.
            int inserted = jdbc.update("""
                    INSERT INTO job_platform.idempotency_keys
                        (client_id, operation_scope, idempotency_key, request_hash, response_status, response_body)
                    VALUES (?, ?, ?, ?, 0, '{}'::jsonb)
                    ON CONFLICT (client_id, operation_scope, idempotency_key) DO NOTHING
                    """, CLIENT, scope, key, hash);
            if (inserted == 0) {
                replayed.set(true);
                // A new READ COMMITTED statement sees the winner after the
                // conflicting insert waited. No aborted-transaction reread.
                return jdbc.queryForObject("""
                        SELECT request_hash, response_status, response_body::text
                        FROM job_platform.idempotency_keys
                        WHERE client_id = ? AND operation_scope = ? AND idempotency_key = ?
                        """, (row, number) -> {
                    if (!hash.equals(row.getString(1))) {
                        throw new ApiException(409, "IDEMPOTENCY_KEY_REUSED",
                                "This key was already used with different input in this operation");
                    }
                    return new SubmissionOutcome(row.getInt(2), parseStoredJson(row.getString(3)));
                }, CLIENT, scope, key);
            }

            SubmissionOutcome outcome = operation.get();
            int updated = jdbc.update("""
                    UPDATE job_platform.idempotency_keys SET response_status = ?, response_body = ?::jsonb
                    WHERE client_id = ? AND operation_scope = ? AND idempotency_key = ?
                    """, outcome.status(), outcome.body().toString(), CLIENT, scope, key);
            if (updated != 1) { throw new IllegalStateException("Request outcome was not finalized"); }
            return outcome;
        })); // execute commits before observations or HTTP return.
        telemetry.request(scope.startsWith("RETRY_JOB:") ? "RETRY_JOB" : scope, replayed.get(), result.status());
        return result;
    }

    private SubmissionOutcome createSingle(JsonNode request) {
        try {
            validator.validate(request);
        } catch (ApiException invalid) {
            return new SubmissionOutcome(invalid.getStatus(), ApiJson.error(invalid.getCode(), invalid.getMessage()));
        }
        UUID id = UUID.randomUUID();
        persistence.createJob(id, request.get("type").textValue(), request.get("payload"));
        return new SubmissionOutcome(202, ApiJson.accepted(id));
    }

    private SubmissionOutcome createBatch(JsonNode request) {
        List<ApiException> errors = new ArrayList<>();
        // Validate every item before inserting any jobs. Replay bypasses this.
        for (JsonNode item : request.get("items")) {
            ApiException error = null;
            try { validator.validate(item); }
            catch (ApiException invalid) { error = invalid; }
            errors.add(error);
        }
        ObjectNode response = mapper.createObjectNode();
        var items = response.putArray("items");
        int accepted = 0;
        for (int index = 0; index < errors.size(); index++) {
            ObjectNode result = items.addObject().put("index", index);
            ApiException error = errors.get(index);
            if (error != null) {
                result.put("outcome", "REJECTED");
                result.set("error", ApiJson.error(error.getCode(), error.getMessage()).get("error"));
            } else {
                JsonNode item = request.get("items").get(index);
                UUID id = UUID.randomUUID();
                persistence.createJob(id, item.get("type").textValue(), item.get("payload"));
                result.put("outcome", "ACCEPTED").setAll(ApiJson.accepted(id));
                accepted++;
            }
        }
        response.put("acceptedCount", accepted).put("rejectedCount", errors.size() - accepted);
        return new SubmissionOutcome(accepted == 0 ? 400 : 202, response);
    }

    private JsonNode parseStoredJson(String body) {
        try { return mapper.readTree(body); }
        catch (JsonProcessingException exception) { throw new IllegalStateException("Invalid stored response", exception); }
    }
}