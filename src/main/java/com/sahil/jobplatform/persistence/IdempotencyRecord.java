package com.sahil.jobplatform.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "idempotency_keys", schema = "job_platform")
public class IdempotencyRecord {

    @EmbeddedId
    private IdempotencyRecordId id;

    @Column(name = "request_hash", nullable = false, columnDefinition = "text")
    private String requestHash;

    @Column(name = "response_status", nullable = false)
    private int responseStatus;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "response_body", nullable = false, columnDefinition = "jsonb")
    private JsonNode responseBody;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected IdempotencyRecord() {
    }

    public IdempotencyRecordId getId() { return id; }
    public String getRequestHash() { return requestHash; }
    public int getResponseStatus() { return responseStatus; }
    public JsonNode getResponseBody() { return responseBody.deepCopy(); }
    public Instant getCreatedAt() { return createdAt; }
}