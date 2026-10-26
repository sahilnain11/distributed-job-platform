package com.sahil.jobplatform.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.sahil.jobplatform.observability.JobTelemetry;
import jakarta.persistence.EntityManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class JobPersistenceService {

    private final EntityManager entityManager;
    private final JdbcTemplate jdbc;
    private final JobRepository jobs;
    private final JobPayloadRepository payloads;
    private final JobTelemetry telemetry;

    public JobPersistenceService(EntityManager entityManager, JdbcTemplate jdbc,
                                 JobRepository jobs, JobPayloadRepository payloads, JobTelemetry telemetry) {
        this.entityManager = entityManager;
        this.jdbc = jdbc;
        this.jobs = jobs;
        this.payloads = payloads;
        this.telemetry = telemetry;
    }

    /**
     * Persistence primitive, not client submission or idempotency handling.
     * REQUIRED propagation lets Phase 3 include the saved request outcome in
     * this same transaction; never publish to the broker from here.
     */
    @Transactional
    public JobDetails createJob(UUID jobId, String type, JsonNode payload) {
        Objects.requireNonNull(jobId, "jobId is required");
        if (type == null || type.isBlank() || type.length() > 255) {
            throw new IllegalArgumentException("type must contain 1 to 255 nonblank characters");
        }
        if (payload == null || payload.isNull() || payload.isMissingNode()) {
            throw new IllegalArgumentException("payload is required");
        }

        Instant createdAt = Objects.requireNonNull(jdbc.queryForObject(
                "SELECT clock_timestamp()", OffsetDateTime.class)).toInstant();
        Job job = new Job(jobId, type, createdAt);
        entityManager.persist(job);
        // UUID references avoid a redundant jobs.payload_id pointer. Flush the
        // parent before dependent inserts, without committing the transaction.
        entityManager.flush();
        JobPayload storedPayload = new JobPayload(UUID.randomUUID(), jobId, payload, createdAt);
        entityManager.persist(storedPayload);
        entityManager.persist(new OutboxEvent(UUID.randomUUID(), jobId, createdAt));
        entityManager.flush();

        telemetry.created(jobId, type);
        return JobDetails.from(job, storedPayload.getPayload());
    }

    @Transactional(readOnly = true)
    public Optional<JobDetails> findJob(UUID jobId) {
        Objects.requireNonNull(jobId, "jobId is required");
        return jobs.findById(jobId).map(job -> {
            JobPayload payload = payloads.findByJobId(jobId)
                    .orElseThrow(() -> new IllegalStateException("Job " + jobId + " has no payload"));
            return JobDetails.from(job, payload.getPayload());
        });
    }
}