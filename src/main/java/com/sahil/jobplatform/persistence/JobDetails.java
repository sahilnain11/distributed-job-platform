package com.sahil.jobplatform.persistence;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/** Detached data for internal callers, not an HTTP response contract. */
public record JobDetails(
        UUID jobId,
        String type,
        JobStatus status,
        JsonNode payload,
        int attemptCount,
        int cycleAttemptCount,
        UUID attemptId,
        Instant leaseExpiry,
        Instant nextRetryAt,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt) {

    public JobDetails {
        payload = payload.deepCopy();
    }

    @Override
    public JsonNode payload() {
        return payload.deepCopy();
    }

    static JobDetails from(Job job, JsonNode payload) {
        return new JobDetails(job.getJobId(), job.getType(), job.getStatus(), payload,
                job.getAttemptCount(), job.getCycleAttemptCount(), job.getAttemptId(),
                job.getLeaseExpiry(), job.getNextRetryAt(), job.getCreatedAt(),
                job.getStartedAt(), job.getCompletedAt());
    }
}