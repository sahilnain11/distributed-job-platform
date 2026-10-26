package com.sahil.jobplatform.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox", schema = "job_platform")
public class OutboxEvent {

    @Id
    @Column(name = "outbox_id", nullable = false, updatable = false)
    private UUID outboxId;

    @Column(name = "job_id", nullable = false, updatable = false)
    private UUID jobId;

    @Column(nullable = false)
    private boolean published;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEvent() {
    }

    OutboxEvent(UUID outboxId, UUID jobId, Instant createdAt) {
        this.outboxId = outboxId;
        this.jobId = jobId;
        this.createdAt = createdAt;
    }

    public UUID getOutboxId() { return outboxId; }
    public UUID getJobId() { return jobId; }
    public boolean isPublished() { return published; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
}