package com.sahil.jobplatform.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Process-local observations, never the authority for job state. */
@Component
public class JobTelemetry {

    private static final Logger LOG = LoggerFactory.getLogger(JobTelemetry.class);
    private static final Set<String> TYPES = Set.of("ECHO", "SIMULATED_WORK");
    private static final Set<String> FAILURES = Set.of("NONE", "TIMEOUT", "LEASE_EXPIRED",
            "SIMULATED_FAILURE", "INVALID_PAYLOAD", "MISSING_PAYLOAD", "UNSUPPORTED_JOB_TYPE");
    private static final Set<String> OUTCOMES = Set.of("COMPLETED", "FAILED", "EXPIRED");
    private static final Set<String> OPERATIONS = Set.of("SUBMIT_JOB", "SUBMIT_BATCH", "RETRY_JOB");
    private final MeterRegistry registry;
    private final AtomicInteger active = new AtomicInteger();

    public JobTelemetry(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder("jobs.worker.active", active, AtomicInteger::get)
                .description("Handlers currently occupying local execution slots").register(registry);
    }

    public void afterCommit(Runnable observation) {
        if (TransactionSynchronizationManager.isSynchronizationActive()
                && TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() { safe(observation); }
            });
        } else { safe(observation); }
    }

    public void created(UUID jobId, String type) {
        afterCommit(() -> {
            registry.counter("jobs.accepted", "type", type(type)).increment();
            LOG.atInfo().addKeyValue("event", "job_accepted").addKeyValue("jobId", jobId)
                    .addKeyValue("jobType", type(type)).log("Job accepted in committed transaction");
        });
    }

    public void claimed(UUID jobId, UUID attemptId, String type, int cycle,
                        OffsetDateTime createdAt, OffsetDateTime startedAt, int lifetime) {
        afterCommit(() -> {
            registry.counter("jobs.attempts.started", "type", type(type)).increment();
            if (lifetime == 1) {
                Timer.builder("jobs.first.start.delay").tag("type", type(type))
                        .description("Creation-to-first-claim delay; includes acceptance transaction time, not exact commit delay")
                        .register(registry).record(nonnegative(Duration.between(createdAt, startedAt)));
            }
            LOG.atInfo().addKeyValue("event", "attempt_started").addKeyValue("jobId", jobId)
                    .addKeyValue("attemptId", attemptId).addKeyValue("jobType", type(type))
                    .addKeyValue("cycleAttempt", cycle).log("Attempt claim committed");
        });
    }

    public void outcome(UUID jobId, UUID attemptId, String state, String jobState, String failure,
                        OffsetDateTime startedAt, OffsetDateTime endedAt) {
        afterCommit(() -> {
            registry.counter("jobs.attempt.outcomes", "outcome", outcome(state), "failure", failure(failure)).increment();
            Timer.builder("jobs.attempt.duration").tag("outcome", outcome(state))
                    .description("Recorded attempt start-to-terminal-outcome duration, including persistence and expiry detection")
                    .register(registry).record(nonnegative(Duration.between(startedAt, endedAt)));
            if (jobState.equals("COMPLETED") || jobState.equals("FAILED")) {
                registry.counter("jobs.terminal", "outcome", jobState).increment();
            }
            LOG.atInfo().addKeyValue("event", "attempt_outcome").addKeyValue("jobId", jobId)
                    .addKeyValue("attemptId", attemptId).addKeyValue("attemptStatus", state)
                    .addKeyValue("jobStatus", jobState).addKeyValue("failureType", failure(failure))
                    .log("Attempt outcome committed");
        });
    }

    public void retry(UUID jobId, boolean manual) {
        afterCommit(() -> {
            registry.counter("jobs.retry.events", "kind", manual ? "manual" : "automatic").increment();
            LOG.atInfo().addKeyValue("event", "retry_queued").addKeyValue("jobId", jobId)
                    .addKeyValue("manual", manual).log("Retry and outbox event committed");
        });
    }

    public void heartbeat(UUID jobId, UUID attemptId) {
        afterCommit(() -> registry.counter("jobs.heartbeat.renewed").increment());
    }

    public void request(String operation, boolean replay, int status) {
        safe(() -> registry.counter("jobs.api.outcomes", "operation", OPERATIONS.contains(operation) ? operation : "OTHER",
            "replay", Boolean.toString(replay), "status", status >= 100 && status <= 599 ? Integer.toString(status) : "OTHER").increment());
    }

    public void workerStarted() { safe(active::incrementAndGet); }
    public void workerFinished() { safe(active::decrementAndGet); }

    public void safe(Runnable action) {
        try { action.run(); }
        catch (RuntimeException failure) {
            // A telemetry exporter/listener must never turn a committed job into
            // an apparent failure, change ACK decisions, or break a heartbeat.
            LOG.warn("Telemetry observation failed: {}", failure.getClass().getSimpleName());
        }
    }

    private String type(String value) { return TYPES.contains(value) ? value : "OTHER"; }
    private String outcome(String value) { return OUTCOMES.contains(value) ? value : "OTHER"; }
    private String failure(String value) { return value == null ? "NONE" : FAILURES.contains(value) ? value : "OTHER"; }
    private Duration nonnegative(Duration value) { return value.isNegative() ? Duration.ZERO : value; }
}