package com.sahil.jobplatform.persistence;

import org.springframework.data.repository.Repository;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends Repository<OutboxEvent, UUID> {
    List<OutboxEvent> findByJobIdOrderByCreatedAtAsc(UUID jobId);
}