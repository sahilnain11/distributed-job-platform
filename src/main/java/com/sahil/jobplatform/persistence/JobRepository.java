package com.sahil.jobplatform.persistence;

import org.springframework.data.repository.Repository;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobRepository extends Repository<Job, UUID> {
    Optional<Job> findById(UUID jobId);

    List<Job> findAllBy(Pageable pageable);

    List<Job> findByStatus(JobStatus status, Pageable pageable);
}