package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "platform.messaging.enabled", havingValue = "true")
public class OutboxPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxPublisher.class);
    private final JdbcTemplate jdbc;
    private final RabbitTemplate rabbit;
    private final ObjectMapper mapper;
    private final MessagingSettings settings;
    private final MeterRegistry metrics;

    public OutboxPublisher(JdbcTemplate jdbc, RabbitTemplate rabbit, ObjectMapper mapper,
                           MessagingSettings settings, MeterRegistry metrics) {
        this.jdbc = jdbc;
        this.rabbit = rabbit;
        this.mapper = mapper;
        this.settings = settings;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${platform.messaging.publisher-delay-ms:1000}")
    public void publishDueEvents() {
        if (!settings.publisherEnabled()) { return; }
        try { publishBatch(); }
        catch (RuntimeException failure) {
            metrics.counter("jobs.outbox.failures").increment();
            LOG.warn("Outbox batch failed: {}", failure.getClass().getSimpleName());
        }
    }

    public int publishBatch() {
        var candidates = jdbc.query("""
            SELECT o.outbox_id, o.job_id FROM job_platform.outbox o
            WHERE o.published = false
                ORDER BY o.created_at, o.outbox_id LIMIT ?
                """, (row, number) -> new Event(row.getObject(1, UUID.class), row.getObject(2, UUID.class)),
                settings.batchSize());
        int published = 0;
        for (Event event : candidates) {
            if (publish(event)) { published++; }
            else { break; } // Do not spin through a broken broker/routing setup.
        }
        return published;
    }

    private boolean publish(Event event) {
        try {
            byte[] body = mapper.createObjectNode().put("jobId", event.jobId().toString())
                    .toString().getBytes(StandardCharsets.UTF_8);
            var message = MessageBuilder.withBody(body)
                    .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                    .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                    .setMessageId(event.outboxId().toString()).build();
            // Unique correlation per send, even when multiple publishers resend
            // the same event. No database transaction is held across this wait.
            CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
            rabbit.send(settings.exchange(), settings.routingKey(), message, correlation);
            var confirmation = correlation.getFuture().get(settings.confirmTimeoutMs(), TimeUnit.MILLISECONDS);
            if (!confirmation.isAck() || correlation.getReturned() != null) {
                throw new IllegalStateException("Publication was not confirmed and routed");
            }
            jdbc.update("""
                    UPDATE job_platform.outbox SET published = true, published_at = clock_timestamp()
                    WHERE outbox_id = ? AND published = false
                    """, event.outboxId());
            metrics.counter("jobs.outbox.confirmed").increment();
            LOG.atInfo().addKeyValue("event", "outbox_published").addKeyValue("outboxId", event.outboxId())
                    .addKeyValue("jobId", event.jobId()).log("Broker publication confirmed and recorded");
            return true;
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            metrics.counter("jobs.outbox.failures").increment();
            LOG.warn("Outbox event {} remains eligible after failure: {}", event.outboxId(),
                    failure.getClass().getSimpleName());
            return false;
        }
    }

    private record Event(UUID outboxId, UUID jobId) { }
}