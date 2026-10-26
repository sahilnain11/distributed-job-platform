package com.sahil.jobplatform.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Watches active attempts independently of the synchronous handler threads. */
@Component
public class AttemptSupervisor {

    private static final Logger LOG = LoggerFactory.getLogger(AttemptSupervisor.class);
    private final JobExecutionStore store;
    private final ReliabilitySettings settings;
    private final ScheduledThreadPoolExecutor executor;
    private final Set<Watch> active = ConcurrentHashMap.newKeySet();
    private volatile boolean closing;

    public AttemptSupervisor(JobExecutionStore store, ReliabilitySettings settings,
                             ScheduledThreadPoolExecutor executor) {
        this.store = store;
        this.settings = settings;
        this.executor = executor;
    }

    public Watch watch(JobExecutionStore.Claim claim, int leaseSeconds) {
        if (closing) { throw new IllegalStateException("Worker is shutting down"); }
        if (settings.heartbeatMs() >= leaseSeconds * 1000L) {
            throw new IllegalStateException("Heartbeat interval must be shorter than the lease");
        }
        var watch = new Watch(claim, leaseSeconds, Thread.currentThread());
        active.add(watch);
        try {
            watch.task = executor.scheduleWithFixedDelay(watch::tick,
                    settings.controlIntervalMs(), settings.controlIntervalMs(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException shutdown) {
            active.remove(watch);
            throw new IllegalStateException("Attempt supervisor is shutting down", shutdown);
        }
        if (closing) { watch.shutdown(); }
        return watch;
    }

    @EventListener
    public void shutdown(ContextClosedEvent event) {
        closing = true;
        for (Watch watch : active) { watch.shutdown(); }
    }

    public final class Watch implements AutoCloseable {
        private final JobExecutionStore.Claim claim;
        private final int leaseSeconds;
        private final Thread handler;
        private ScheduledFuture<?> task;
        private long lastHeartbeat = System.nanoTime();
        private volatile boolean cancelled;
        private boolean finished;
        private boolean uncertain;

        private Watch(JobExecutionStore.Claim claim, int leaseSeconds, Thread handler) {
            this.claim = claim;
            this.leaseSeconds = leaseSeconds;
            this.handler = handler;
        }

        private synchronized void tick() {
            if (finished || cancelled || closing) { return; }
            boolean renew = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastHeartbeat) >= settings.heartbeatMs();
            try {
                var state = store.supervise(claim.jobId(), claim.attemptId(), leaseSeconds,
                        settings.maxRuntimeMs(), renew, settings.retryBaseMs());
                if (state != JobExecutionStore.Supervision.OWNED) {
                    cancelled = true;
                    LOG.atWarn().addKeyValue("event", "attempt_cancellation_requested")
                            .addKeyValue("jobId", claim.jobId()).addKeyValue("attemptId", claim.attemptId())
                            .addKeyValue("reason", state.name()).log("Cooperative attempt cancellation requested");
                    handler.interrupt();
                } else if (renew) { lastHeartbeat = System.nanoTime(); }
            } catch (RuntimeException failure) {
                uncertain = true;
                cancelled = true;
                handler.interrupt();
                LOG.atWarn().addKeyValue("event", "attempt_supervision_unavailable")
                    .addKeyValue("jobId", claim.jobId()).addKeyValue("attemptId", claim.attemptId())
                    .addKeyValue("exceptionType", failure.getClass().getSimpleName())
                    .log("Attempt supervision unavailable; refusing uncertain writes");
            }
        }

        public boolean cancelled() { return cancelled || closing; }

        /** Serializes finalization with this attempt's in-flight control task. */
        public synchronized boolean finish(BooleanSupplier write) {
            finished = true;
            if (task != null) { task.cancel(false); }
            if (uncertain || closing) { throw new IllegalStateException("Attempt outcome needs reconciliation"); }
            if (cancelled) { return false; }
            var state = store.supervise(claim.jobId(), claim.attemptId(), leaseSeconds,
                    settings.maxRuntimeMs(), false, settings.retryBaseMs());
            return state == JobExecutionStore.Supervision.OWNED && write.getAsBoolean();
        }

        private synchronized void shutdown() {
            cancelled = true;
            uncertain = true;
            if (task != null) { task.cancel(false); }
            handler.interrupt();
        }

        @Override
        public synchronized void close() {
            finished = true;
            if (task != null) { task.cancel(false); }
            active.remove(this);
            if (cancelled && !closing) { Thread.interrupted(); }
        }
    }
}