package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sahil.jobplatform.persistence.AttemptStatus;
import com.sahil.jobplatform.persistence.JobAttemptRepository;
import com.sahil.jobplatform.persistence.JobPersistenceService;
import com.sahil.jobplatform.persistence.JobRepository;
import com.sahil.jobplatform.persistence.JobResultRepository;
import com.sahil.jobplatform.persistence.JobStatus;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class JobQueryService {

    private final ObjectMapper mapper;
    private final JobPersistenceService persistence;
    private final JobRepository jobs;
    private final JobAttemptRepository attempts;
    private final JobResultRepository results;

    public JobQueryService(ObjectMapper mapper, JobPersistenceService persistence, JobRepository jobs,
                           JobAttemptRepository attempts, JobResultRepository results) {
        this.mapper = mapper;
        this.persistence = persistence;
        this.jobs = jobs;
        this.attempts = attempts;
        this.results = results;
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public ObjectNode getJob(UUID id) {
        var job = persistence.findJob(id).orElseThrow(() ->
                new ApiException(404, "JOB_NOT_FOUND", "Job does not exist"));
        ObjectNode body = mapper.valueToTree(job);
        body.putNull("failure");
        body.putNull("result");
        var history = body.putArray("attempts");
        for (var attempt : attempts.findByJobIdOrderByAttemptNumberAsc(id)) {
            var item = history.addObject().put("attemptId", attempt.getAttemptId().toString())
                .put("attemptNumber", attempt.getAttemptNumber()).put("workerId", attempt.getWorkerId().toString())
                .put("status", attempt.getStatus().name());
            item.set("startedAt", mapper.valueToTree(attempt.getStartedAt()));
            item.set("endedAt", mapper.valueToTree(attempt.getEndedAt()));
            item.put("failureType", attempt.getFailureType()).put("failureReason", attempt.getFailureReason());
        }
        if (job.status() == JobStatus.COMPLETED) {
            results.findByJobId(id).ifPresent(result -> {
                ObjectNode stored = mapper.createObjectNode().put("storageType", result.getStorageType());
                stored.set("value", result.getResult());
                stored.put("location", result.getLocation());
                body.set("result", stored);
            });
        } else if ((job.status() == JobStatus.FAILED || job.status() == JobStatus.RETRYING)
                && job.cycleAttemptCount() > 0 && job.attemptId() != null) {
            attempts.findById(job.attemptId()).filter(attempt -> id.equals(attempt.getJobId()))
                    .filter(attempt -> attempt.getStatus() == AttemptStatus.FAILED
                            || attempt.getStatus() == AttemptStatus.EXPIRED)
                    .ifPresent(attempt -> body.putObject("failure")
                            .put("type", attempt.getFailureType()).put("reason", attempt.getFailureReason()));
        }
        return body;
    }

    @Transactional(readOnly = true)
    public ObjectNode listJobs(JobStatus status, int limit) {
        ApiJson.require(limit >= 1 && limit <= 100, "INVALID_LIMIT", "limit must be between 1 and 100");
        var page = PageRequest.of(0, limit, Sort.by(Sort.Order.desc("createdAt"), Sort.Order.asc("jobId")));
        var found = status == null ? jobs.findAllBy(page) : jobs.findByStatus(status, page);
        ObjectNode response = mapper.createObjectNode();
        var list = response.putArray("jobs");
        for (var job : found) {
            ObjectNode summary = list.addObject().put("jobId", job.getJobId().toString())
                    .put("type", job.getType()).put("status", job.getStatus().name())
                    .put("attemptCount", job.getAttemptCount());
            summary.set("createdAt", mapper.valueToTree(job.getCreatedAt()));
        }
        response.put("limit", limit);
        return response;
    }
}