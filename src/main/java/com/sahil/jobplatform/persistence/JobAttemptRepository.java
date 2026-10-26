package com.sahil.jobplatform.persistence;

import org.springframework.data.repository.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobAttemptRepository extends Repository<JobAttempt, UUID> {
    Optional<JobAttempt> findById(UUID attemptId);

    List<JobAttempt> findByJobIdOrderByAttemptNumberAsc(UUID jobId);
}