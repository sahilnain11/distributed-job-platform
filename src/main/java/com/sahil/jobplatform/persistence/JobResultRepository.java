package com.sahil.jobplatform.persistence;

import org.springframework.data.repository.Repository;

import java.util.Optional;
import java.util.UUID;

public interface JobResultRepository extends Repository<JobResult, UUID> {
    Optional<JobResult> findByJobId(UUID jobId);
}