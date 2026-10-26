package com.sahil.jobplatform.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class IdempotencyRecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    @Column(name = "client_id", nullable = false, columnDefinition = "text")
    private String clientId;

    @Column(name = "operation_scope", nullable = false, columnDefinition = "text")
    private String operationScope;

    @Column(name = "idempotency_key", nullable = false, columnDefinition = "text")
    private String idempotencyKey;

    protected IdempotencyRecordId() {
    }

    public IdempotencyRecordId(String clientId, String operationScope, String idempotencyKey) {
        this.clientId = Objects.requireNonNull(clientId);
        this.operationScope = Objects.requireNonNull(operationScope);
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey);
    }

    public String getClientId() { return clientId; }
    public String getOperationScope() { return operationScope; }
    public String getIdempotencyKey() { return idempotencyKey; }

    @Override
    public boolean equals(Object other) {
        if (this == other) { return true; }
        if (!(other instanceof IdempotencyRecordId that)) { return false; }
        return Objects.equals(clientId, that.clientId)
                && Objects.equals(operationScope, that.operationScope)
                && Objects.equals(idempotencyKey, that.idempotencyKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, operationScope, idempotencyKey);
    }
}