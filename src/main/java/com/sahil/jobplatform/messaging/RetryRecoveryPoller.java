package com.sahil.jobplatform.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "platform.messaging.enabled", havingValue = "true")
public class RetryRecoveryPoller {

    private static final Logger LOG = LoggerFactory.getLogger(RetryRecoveryPoller.class);
    private final JobExecutionStore store;
    private final ReliabilitySettings reliability;
    private final MessagingSettings messaging;
    private final MeterRegistry metrics;

    public RetryRecoveryPoller(JobExecutionStore store, ReliabilitySettings reliability,
                               MessagingSettings messaging, MeterRegistry metrics) {
        this.store = store;
        this.reliability = reliability;
        this.messaging = messaging;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${platform.reliability.retry-poll-ms:1000}")
    public void scheduleRetries() {
        if (!reliability.pollersEnabled()) { return; }
        try {
            for (var id : store.dueRetries(messaging.batchSize())) {
                if (store.queueRetry(id)) { metrics.counter("jobs.retries.queued").increment(); }
            }
        } catch (RuntimeException failure) { log(failure); }
    }

    @Scheduled(fixedDelayString = "${platform.reliability.recovery-poll-ms:10000}")
    public void recoverExpired() {
        if (!reliability.pollersEnabled()) { return; }
        try {
            for (var id : store.expiredJobs(messaging.batchSize())) {
                if (store.recover(id, reliability.retryBaseMs())) { metrics.counter("jobs.attempts.expired").increment(); }
            }
        } catch (RuntimeException failure) { log(failure); }
    }

    private void log(RuntimeException failure) {
        LOG.warn("Retry/recovery poll failed: {}", failure.getClass().getSimpleName());
    }
}