package com.sahil.jobplatform.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "job_payloads", schema = "job_platform")
public class JobPayload {

    @Id
    @Column(name = "payload_id", nullable = false, updatable = false)
    private UUID payloadId;

    @Column(name = "job_id", nullable = false, unique = true, updatable = false)
    private UUID jobId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb", updatable = false)
    private JsonNode payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected JobPayload() {
    }

    JobPayload(UUID payloadId, UUID jobId, JsonNode payload, Instant createdAt) {
        this.payloadId = payloadId;
        this.jobId = jobId;
        this.payload = payload.deepCopy();
        this.createdAt = createdAt;
    }

    public UUID getPayloadId() { return payloadId; }
    public UUID getJobId() { return jobId; }
    public JsonNode getPayload() { return payload.deepCopy(); }
    public Instant getCreatedAt() { return createdAt; }
}