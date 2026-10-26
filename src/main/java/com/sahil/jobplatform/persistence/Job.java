package com.sahil.jobplatform.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "jobs", schema = "job_platform")
public class Job {

    @Id
    @Column(name = "job_id", nullable = false, updatable = false)
    private UUID jobId;

    @Column(nullable = false, updatable = false)
    private String type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "cycle_attempt_count", nullable = false)
    private int cycleAttemptCount;

    @Column(name = "attempt_id")
    private UUID attemptId;

    @Column(name = "lease_expiry")
    private Instant leaseExpiry;

    @Column(name = "next_retry_at")
    private Instant nextRetryAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    protected Job() {
    }

    Job(UUID jobId, String type, Instant createdAt) {
        this.jobId = jobId;
        this.type = type;
        this.status = JobStatus.QUEUED;
        this.createdAt = createdAt;
    }

    public UUID getJobId() { return jobId; }
    public String getType() { return type; }
    public JobStatus getStatus() { return status; }
    public int getAttemptCount() { return attemptCount; }
    public int getCycleAttemptCount() { return cycleAttemptCount; }
    public UUID getAttemptId() { return attemptId; }
    public Instant getLeaseExpiry() { return leaseExpiry; }
    public Instant getNextRetryAt() { return nextRetryAt; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getCompletedAt() { return completedAt; }
}