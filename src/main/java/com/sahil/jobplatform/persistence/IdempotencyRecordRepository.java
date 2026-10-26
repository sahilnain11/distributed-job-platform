package com.sahil.jobplatform.persistence;

import org.springframework.data.repository.Repository;

import java.util.Optional;

public interface IdempotencyRecordRepository extends Repository<IdempotencyRecord, IdempotencyRecordId> {
    Optional<IdempotencyRecord> findById(IdempotencyRecordId id);
}