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
@Table(name = "job_attempts", schema = "job_platform")
public class JobAttempt {

    @Id
    @Column(name = "attempt_id", nullable = false, updatable = false)
    private UUID attemptId;

    @Column(name = "job_id", nullable = false, updatable = false)
    private UUID jobId;

    @Column(name = "attempt_number", nullable = false, updatable = false)
    private int attemptNumber;

    @Column(name = "worker_id", nullable = false, updatable = false)
    private UUID workerId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private AttemptStatus status;

    @Column(name = "started_at", nullable = false, updatable = false)
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    @Column(name = "failure_type")
    private String failureType;

    @Column(name = "failure_reason", columnDefinition = "text")
    private String failureReason;

    protected JobAttempt() {
    }

    public UUID getAttemptId() { return attemptId; }
    public UUID getJobId() { return jobId; }
    public int getAttemptNumber() { return attemptNumber; }
    public UUID getWorkerId() { return workerId; }
    public AttemptStatus getStatus() { return status; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getEndedAt() { return endedAt; }
    public String getFailureType() { return failureType; }
    public String getFailureReason() { return failureReason; }
}