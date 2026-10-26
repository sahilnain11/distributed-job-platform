package com.sahil.jobplatform.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.sahil.jobplatform.persistence.JobStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobSubmissionService submissions;
    private final JobQueryService queries;

    public JobController(JobSubmissionService submissions, JobQueryService queries) {
        this.submissions = submissions;
        this.queries = queries;
    }

    @PostMapping(consumes = "application/json", produces = "application/json")
    public ResponseEntity<JsonNode> submit(@RequestHeader HttpHeaders headers, @RequestBody JsonNode request) {
        SubmissionOutcome outcome = submissions.submit(key(headers), request, false);
        var response = ResponseEntity.status(outcome.status());
        if (outcome.status() == 202) {
            response.location(URI.create("/jobs/" + outcome.body().get("jobId").asText()));
        }
        return response.body(outcome.body());
    }

    @PostMapping(path = "/batch", consumes = "application/json", produces = "application/json")
    public ResponseEntity<JsonNode> batch(@RequestHeader HttpHeaders headers, @RequestBody JsonNode request) {
        SubmissionOutcome outcome = submissions.submit(key(headers), request, true);
        return ResponseEntity.status(outcome.status()).body(outcome.body());
    }

    @GetMapping(path = "/{jobId}", produces = "application/json")
    public JsonNode get(@PathVariable UUID jobId) {
        return queries.getJob(jobId);
    }

    @PostMapping(path = "/{jobId}/retry", produces = "application/json")
    public ResponseEntity<JsonNode> retry(@PathVariable UUID jobId, @RequestHeader HttpHeaders headers,
                                         @RequestBody(required = false) JsonNode request) {
        SubmissionOutcome outcome = submissions.retry(key(headers), jobId, request);
        var response = ResponseEntity.status(outcome.status());
        if (outcome.status() == 202) { response.location(URI.create("/jobs/" + jobId)); }
        return response.body(outcome.body());
    }
    @GetMapping(produces = "application/json")
    public JsonNode list(@RequestParam(required = false) JobStatus status,
                         @RequestParam(defaultValue = "100") int limit) {
        return queries.listJobs(status, limit);
    }

    private String key(HttpHeaders headers) {
        var keys = headers.get("Idempotency-Key");
        ApiJson.require(keys != null && keys.size() == 1, "INVALID_IDEMPOTENCY_KEY",
                "Exactly one Idempotency-Key header is required");
        return keys.getFirst();
    }
}