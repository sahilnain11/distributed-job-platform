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
@Table(name = "job_results", schema = "job_platform")
public class JobResult {

    @Id
    @Column(name = "result_id", nullable = false, updatable = false)
    private UUID resultId;

    @Column(name = "job_id", nullable = false, unique = true, updatable = false)
    private UUID jobId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private JsonNode result;

    @Column(name = "storage_type", nullable = false)
    private String storageType;

    @Column(columnDefinition = "text")
    private String location;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected JobResult() {
    }

    public UUID getResultId() { return resultId; }
    public UUID getJobId() { return jobId; }
    public JsonNode getResult() { return result == null ? null : result.deepCopy(); }
    public String getStorageType() { return storageType; }
    public String getLocation() { return location; }
    public Instant getCreatedAt() { return createdAt; }
}