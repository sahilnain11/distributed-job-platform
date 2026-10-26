package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.sahil.jobplatform.observability.JobTelemetry;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class JobWorker {

    public enum Outcome { ACK, REJECT, PAUSE }
    private final JobExecutionStore store;
    private final AttemptSupervisor supervisor;
    private final ReliabilitySettings settings;
    private final JobTelemetry telemetry;
    private final UUID workerId = UUID.randomUUID();

    public JobWorker(JobExecutionStore store, AttemptSupervisor supervisor, ReliabilitySettings settings, JobTelemetry telemetry) {
        this.store = store;
        this.supervisor = supervisor;
        this.settings = settings;
        this.telemetry = telemetry;
    }

    public Outcome process(UUID jobId, int leaseSeconds) {
        var claim = store.claim(jobId, workerId, leaseSeconds);
        return switch (claim.decision()) {
            case DUPLICATE -> Outcome.ACK;
            case MISSING -> Outcome.REJECT;
            case CLAIMED -> execute(claim, leaseSeconds);
        };
    }

    private Outcome execute(JobExecutionStore.Claim claim, int leaseSeconds) {
        telemetry.workerStarted();
        try (var watch = supervisor.watch(claim, leaseSeconds)) {
            boolean saved;
            if (claim.payload() == null) {
                saved = watch.finish(() -> store.failPermanently(claim.jobId(), claim.attemptId(),
                        "MISSING_PAYLOAD", "Stored job has no payload"));
            } else if (!"ECHO".equals(claim.type()) && !"SIMULATED_WORK".equals(claim.type())) {
                saved = watch.finish(() -> store.failPermanently(claim.jobId(), claim.attemptId(),
                        "UNSUPPORTED_JOB_TYPE", "Job type is not supported"));
            } else {
                try {
                    JsonNode result = "ECHO".equals(claim.type()) ? claim.payload() : simulate(claim, watch);
                    saved = watch.finish(() -> store.complete(claim.jobId(), claim.attemptId(), result));
                } catch (SimulatedFailure retryable) {
                    saved = watch.finish(() -> store.failRetryably(claim.jobId(), claim.attemptId(),
                            "SIMULATED_FAILURE", "Configured attempt failure", settings.retryBaseMs()));
                } catch (InvalidSimulation invalid) {
                    saved = watch.finish(() -> store.failPermanently(claim.jobId(), claim.attemptId(),
                            "INVALID_PAYLOAD", "Stored simulation settings are invalid"));
                } catch (InterruptedException cancelled) {
                    // Clear the interruption caused by cooperative supervision;
                    // no additional execution occurs. Shutdown/uncertainty still
                    // refuses ACK via finish(), while expired ownership reconciles.
                    saved = watch.finish(() -> false);
                }
            }
            if (saved || store.mayAcknowledgeOldAttempt(claim.jobId(), claim.attemptId())) { return Outcome.ACK; }
            return Outcome.PAUSE;
        } finally {
            telemetry.workerFinished();
        }
    }

    private JsonNode simulate(JobExecutionStore.Claim claim, AttemptSupervisor.Watch watch) throws InterruptedException {
        long duration;
        int failures;
        try {
            if (!claim.payload().isObject() || !claim.payload().path("durationMillis").isNumber()
                || !claim.payload().path("failFirstAttempts").isNumber()) { throw new InvalidSimulation(); }
            duration = claim.payload().path("durationMillis").decimalValue().longValueExact();
            failures = claim.payload().path("failFirstAttempts").decimalValue().intValueExact();
            if (duration < 0 || failures < 0) { throw new InvalidSimulation(); }
        } catch (ArithmeticException invalid) { throw new InvalidSimulation(); }
        long started = System.nanoTime();
        while (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < duration) {
            if (watch.cancelled()) { throw new InterruptedException("Attempt cancelled"); }
            long remaining = duration - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            if (remaining > 0) { Thread.sleep(Math.min(100, remaining)); }
        }
        if (watch.cancelled()) { throw new InterruptedException("Attempt cancelled"); }
        if (claim.cycleAttempt() <= failures) { throw new SimulatedFailure(); }
        return JsonNodeFactory.instance.objectNode().put("durationMillis", duration)
                .put("cycleAttempt", claim.cycleAttempt()).put("message", "Simulated work completed");
    }

    private static final class SimulatedFailure extends RuntimeException { }
    private static final class InvalidSimulation extends RuntimeException { }
}