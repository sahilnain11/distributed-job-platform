package com.sahil.jobplatform.persistence;

import org.springframework.data.repository.Repository;

import java.util.Optional;
import java.util.UUID;

public interface JobPayloadRepository extends Repository<JobPayload, UUID> {
    Optional<JobPayload> findByJobId(UUID jobId);
}