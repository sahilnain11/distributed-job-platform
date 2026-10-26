package com.sahil.jobplatform.observability;

import com.sahil.jobplatform.messaging.MessagingSettings;
import com.sahil.jobplatform.messaging.WorkerBackoff;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "platform.messaging.enabled", havingValue = "true")
class WorkerMetrics {

    WorkerMetrics(MeterRegistry registry, SimpleMessageListenerContainer container,
                   WorkerBackoff backoff, MessagingSettings settings) {
        Gauge.builder("jobs.worker.running", container, value -> value.isRunning() ? 1 : 0).register(registry);
        Gauge.builder("jobs.worker.consumers", container, SimpleMessageListenerContainer::getActiveConsumerCount)
                .register(registry);
        Gauge.builder("jobs.worker.paused", backoff, value -> value.isPaused() ? 1 : 0).register(registry);
        Gauge.builder("jobs.worker.capacity", settings, MessagingSettings::workerConcurrency).register(registry);
    }
}