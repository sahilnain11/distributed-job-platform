package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
@ConditionalOnProperty(name = "platform.messaging.enabled", havingValue = "true")
public class JobMessageListener implements ChannelAwareMessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(JobMessageListener.class);
    private final ObjectMapper mapper;
    private final JobWorker worker;
    private final MessagingSettings settings;
    private final WorkerBackoff backoff;
    private final MeterRegistry metrics;

    public JobMessageListener(ObjectMapper mapper, JobWorker worker, MessagingSettings settings,
                              WorkerBackoff backoff, MeterRegistry metrics) {
        this.mapper = mapper;
        this.worker = worker;
        this.settings = settings;
        this.backoff = backoff;
        this.metrics = metrics;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        if (backoff.isPaused()) { return; } // unacknowledged until channel stop
        long tag = message.getMessageProperties().getDeliveryTag();
        UUID jobId;
        try {
            var body = mapper.readTree(message.getBody());
            if (body == null || !body.isObject() || body.size() != 1 || !body.path("jobId").isTextual()) {
                throw new IllegalArgumentException("Invalid notification structure");
            }
            String value = body.get("jobId").textValue();
            jobId = UUID.fromString(value);
            if (!jobId.toString().equalsIgnoreCase(value)) { throw new IllegalArgumentException("Invalid UUID"); }
        } catch (Exception malformed) {
            metrics.counter("jobs.messages.invalid").increment();
            LOG.warn("Rejecting malformed job notification without requeue");
            channel.basicReject(tag, false);
            return;
        }

        try {
            var outcome = worker.process(jobId, settings.leaseSeconds());
            switch (outcome) {
                case ACK -> {
                    channel.basicAck(tag, false); // the store transaction already committed
                    metrics.counter("jobs.messages.acknowledged").increment();
                    backoff.successfulDelivery();
                }
                case REJECT -> {
                    metrics.counter("jobs.messages.missing").increment();
                    LOG.warn("Rejecting notification for confirmed missing job {}", jobId);
                    channel.basicReject(tag, false);
                }
                case PAUSE -> backoff.pause(true);
            }
        } catch (RuntimeException databaseOrOutcomeUnknown) {
            metrics.counter("jobs.messages.unresolved").increment();
            LOG.warn("Job {} outcome unresolved: {}; not acknowledging", jobId,
                    databaseOrOutcomeUnknown.getClass().getSimpleName());
            backoff.pause(true);
        }
    }
}