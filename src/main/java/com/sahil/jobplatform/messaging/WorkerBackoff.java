package com.sahil.jobplatform.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(name = "platform.messaging.enabled", havingValue = "true")
public class WorkerBackoff {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerBackoff.class);
    private final ObjectProvider<SimpleMessageListenerContainer> containers;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean paused = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private volatile long delaySeconds = 1;

    public WorkerBackoff(ObjectProvider<SimpleMessageListenerContainer> containers,
                         @Qualifier("workerBackoffExecutor") ScheduledExecutorService executor) {
        this.containers = containers;
        this.executor = executor;
    }

    public boolean isPaused() { return paused.get(); }

    public void successfulDelivery() { delaySeconds = 1; }

    @EventListener
    public void closing(ContextClosedEvent event) {
        closing.set(true);
        paused.set(true);
    }

    public void pause(boolean restart) {
        if (closing.get()) { return; }
        if (!paused.compareAndSet(false, true)) { return; }
        // Never block a consumer waiting for that same consumer to shut down.
        // Return without ACK; channel shutdown lets the broker redeliver later.
        try {
            executor.execute(() -> {
                if (closing.get()) { return; }
                var container = containers.getObject();
                container.stop();
                if (restart && !closing.get()) {
                    long delay = delaySeconds;
                    delaySeconds = Math.min(30, delaySeconds * 2);
                        LOG.atWarn().addKeyValue("event", "worker_intake_paused").addKeyValue("restartDelaySeconds", delay)
                            .log("Worker intake paused for infrastructure recovery");
                    executor.schedule(() -> {
                        if (executor.isShutdown() || closing.get()) { return; }
                        paused.set(false);
                        container.start();
                    }, delay, TimeUnit.SECONDS);
                } else if (!closing.get()) {
                    LOG.error("Worker paused; inspect the unresolved ownership or execution state before restarting");
                }
            });
        } catch (RejectedExecutionException shutdown) {
            // Spring's container lifecycle closes channels on context shutdown.
            // Do not fall back to a blocking stop on the listener's own thread.
            LOG.debug("Worker pause executor unavailable during shutdown");
        }
    }
}