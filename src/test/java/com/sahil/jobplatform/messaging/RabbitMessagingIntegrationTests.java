package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "platform.messaging.enabled=true", "management.health.rabbit.enabled=true",
        "platform.messaging.publisher-enabled=false", "platform.messaging.worker-enabled=false",
        "platform.reliability.pollers-enabled=false"})
class RabbitMessagingIntegrationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    @Container
    @ServiceConnection
    static final RabbitMQContainer RABBIT = new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.1.8-management"))
            .withCopyFileToContainer(MountableFile.forHostPath("docker/rabbitmq.conf"), "/etc/rabbitmq/rabbitmq.conf");

    @Autowired private OutboxPublisher publisher;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private RabbitTemplate rabbit;
    @Autowired private ConnectionFactory connections;
    @Autowired private SimpleMessageListenerContainer container;
    @Autowired private MessagingSettings settings;
    @Autowired private ObjectMapper mapper;
    @Autowired private MeterRegistry metrics;
    @Autowired private TestRestTemplate http;

    @BeforeEach
    void clearTestData() {
        container.stop();
        new RabbitAdmin(connections).purgeQueue(settings.queue(), false);
        jdbc.execute("TRUNCATE idempotency_keys, outbox, job_results, job_attempts, job_payloads, jobs");
    }

    @AfterEach
    void stopConsumer() { container.stop(); }

    @Test
    void durableTopologyPersistentMessageAndConfirmedEventAreCreated() throws Exception {
        UUID id = submit("ECHO", "{\"message\":\"hello\"}");
        assertThat(publisher.publishBatch()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT published AND published_at IS NOT NULL FROM outbox WHERE job_id = ?",
                Boolean.class, id)).isTrue();
        Message delivery = rabbit.receive(settings.queue(), 5000);
        assertThat(delivery).isNotNull();
        assertThat(delivery.getMessageProperties().getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
        assertThat(mapper.readTree(delivery.getBody()).get("jobId").asText()).isEqualTo(id.toString());
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("QUEUED");
        assertThat(RABBIT.execInContainer("rabbitmqctl", "list_queues", "name", "durable").getStdout())
                .contains(settings.queue()).contains("true");
        assertThat(RABBIT.execInContainer("rabbitmqctl", "list_exchanges", "name", "durable").getStdout())
                .contains(settings.exchange());
        assertThat(RABBIT.execInContainer("rabbitmq-diagnostics", "environment").getStdout())
                .contains("600000");
    }

    @Test
    void unroutablePublicationIsNotMarkedPublishedEvenWithPositiveBrokerConfirmation() throws Exception {
        UUID id = submit("ECHO", "{}");
        var wrongRoute = new MessagingSettings(true, false, settings.exchange(), settings.queue(),
                "route.without.binding", 100, 5000, 1, 30);
        assertThat(new OutboxPublisher(jdbc, rabbit, mapper, wrongRoute, metrics).publishBatch()).isZero();
        assertThat(jdbc.queryForObject("SELECT published FROM outbox WHERE job_id = ?", Boolean.class, id)).isFalse();
        assertThat(jdbc.queryForObject("SELECT published_at IS NULL FROM outbox WHERE job_id = ?", Boolean.class, id)).isTrue();
        assertThat(publisher.publishBatch()).isEqualTo(1);
    }

    @Test
    void missingExchangeLeavesEventUnpublishedAndRetryable() throws Exception {
        UUID id = submit("ECHO", "{}");
        var missingExchange = new MessagingSettings(true, false, "nonexistent.exchange", settings.queue(),
                settings.routingKey(), 100, 5000, 1, 30);
        assertThat(new OutboxPublisher(jdbc, rabbit, mapper, missingExchange, metrics).publishBatch()).isZero();
        assertThat(jdbc.queryForObject("SELECT published FROM outbox WHERE job_id = ?", Boolean.class, id)).isFalse();
    }

    @Test
    void fullApiOutboxWorkerResultFlowCompletesOnlyOnceForDuplicateNotifications() throws Exception {
        UUID id = submit("ECHO", "{\"nested\":{\"value\":0.123456789012345678901},\"items\":[3,2,1]}");
        assertThat(publisher.publishBatch()).isEqualTo(1);
        // Crash-after-publish-before-mark simulation: resend the same event.
        jdbc.update("UPDATE outbox SET published=false, published_at=NULL WHERE job_id = ?", id);
        assertThat(publisher.publishBatch()).isEqualTo(1);
        container.start();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("COMPLETED"));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(queueDepth()).isZero());
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM jobs WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_results WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
        var response = http.getForEntity("/jobs/" + id, String.class);
        var job = mapper.readTree(response.getBody());
        assertThat(job.path("result").path("value")).isEqualTo(job.path("payload"));
    }

    @Test
    void realBrokerRedeliveryAfterCompletionBeforeAckDoesNotRunAnotherAttempt() throws Exception {
        UUID id = submit("ECHO", "{}");
        publisher.publishBatch();
        // Use a raw, unpooled channel: closing a Spring cached channel may just
        // return it to the cache instead of forcing broker redelivery.
        try (var connection = ((org.springframework.amqp.rabbit.connection.CachingConnectionFactory) connections)
                .getRabbitConnectionFactory().newConnection()) {
            var channel = connection.createChannel();
            var delivery = channel.basicGet(settings.queue(), false);
            assertThat(delivery).isNotNull();
            // Manually run the worker but deliberately never ACK its delivery.
            // Use the proxied worker below so database transitions commit separately.
            var body = mapper.readTree(delivery.getBody());
            assertThat(body.path("jobId").asText()).isEqualTo(id.toString());
            var worker = applicationWorker();
            assertThat(worker.process(id, 30)).isEqualTo(JobWorker.Outcome.ACK);
            channel.close();
        }
        double acknowledgements = metrics.counter("jobs.messages.acknowledged").count();
        container.start();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
            assertThat(metrics.counter("jobs.messages.acknowledged").count()).isGreaterThan(acknowledgements));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("COMPLETED");
    }

    @Autowired private JobWorker proxiedWorker;
    private JobWorker applicationWorker() { return proxiedWorker; }

    @Test
    void publishedMarkFailureAllowsDuplicateSendButOnlyOneExecution() throws Exception {
        UUID id = submit("ECHO", "{}");
        jdbc.execute("ALTER TABLE outbox ADD CONSTRAINT test_no_published CHECK (published=false)");
        try { assertThat(publisher.publishBatch()).isZero(); }
        finally { jdbc.execute("ALTER TABLE outbox DROP CONSTRAINT test_no_published"); }
        assertThat(jdbc.queryForObject("SELECT published FROM outbox WHERE job_id = ?", Boolean.class, id)).isFalse();
        assertThat(publisher.publishBatch()).isEqualTo(1);
        container.start();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("COMPLETED"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts WHERE job_id = ?", Integer.class, id)).isEqualTo(1);
    }

    @Test
    void malformedAndMissingNotificationsAreDiscardedWithoutCreatingJobs() {
        double invalid = metrics.counter("jobs.messages.invalid").count();
        double missing = metrics.counter("jobs.messages.missing").count();
        rabbit.convertAndSend(settings.exchange(), settings.routingKey(), "invalid-json");
        rabbit.convertAndSend(settings.exchange(), settings.routingKey(), "{\"jobId\":\"" + UUID.randomUUID() + "\"}");
        container.start();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(metrics.counter("jobs.messages.invalid").count()).isGreaterThan(invalid);
            assertThat(metrics.counter("jobs.messages.missing").count()).isGreaterThan(missing);
            assertThat(queueDepth()).isZero();
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Integer.class)).isZero();
    }

    @Test
    void publisherIncludesPreviouslyDeferredSimulationAndEchoEvents() throws Exception {
        UUID simulation = submit("SIMULATED_WORK", "{\"durationMillis\":1000,\"failFirstAttempts\":2}");
        UUID echo = submit("ECHO", "{}");
        assertThat(publisher.publishBatch()).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT published FROM outbox WHERE job_id = ?", Boolean.class, simulation)).isTrue();
        assertThat(jdbc.queryForObject("SELECT published FROM outbox WHERE job_id = ?", Boolean.class, echo)).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, simulation)).isEqualTo("QUEUED");
    }

    @Test
    void negativeOrUnknownBrokerConfirmNeverMarksEventPublished() throws Exception {
        UUID id = submit("ECHO", "{}");
        var transport = org.mockito.Mockito.mock(RabbitTemplate.class);
        org.mockito.Mockito.doAnswer(call -> {
            org.springframework.amqp.rabbit.connection.CorrelationData correlation = call.getArgument(3);
            correlation.getFuture().complete(new org.springframework.amqp.rabbit.connection.CorrelationData.Confirm(false, "nack"));
            return null;
        }).when(transport).send(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any(Message.class),
                org.mockito.ArgumentMatchers.any(org.springframework.amqp.rabbit.connection.CorrelationData.class));
        var shortWait = new MessagingSettings(true, false, settings.exchange(), settings.queue(), settings.routingKey(), 100, 1, 1, 30);
        assertThat(new OutboxPublisher(jdbc, transport, mapper, shortWait, metrics).publishBatch()).isZero();
        org.mockito.Mockito.reset(transport); // no future completion -> unknown outcome/timeout
        assertThat(new OutboxPublisher(jdbc, transport, mapper, shortWait, metrics).publishBatch()).isZero();
        assertThat(jdbc.queryForObject("SELECT published FROM outbox WHERE job_id = ?", Boolean.class, id)).isFalse();
    }

    @Test
    void workerConfigurationBoundsAggregateDeliveriesAndUsesManualAcks() {
        assertThat(container.getAcknowledgeMode()).isEqualTo(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
        assertThat(org.springframework.test.util.ReflectionTestUtils.getField(container, "prefetchCount")).isEqualTo(1);
        assertThat(settings.workerConcurrency()).isEqualTo(10);
    }

    private UUID submit(String type, String payload) throws Exception {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", UUID.randomUUID().toString());
        var response = http.postForEntity("/jobs", new HttpEntity<>("{\"type\":\"" + type + "\",\"payload\":" + payload + "}", headers), String.class);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        return UUID.fromString(mapper.readTree(response.getBody()).path("jobId").asText());
    }

    private int queueDepth() {
        var properties = new RabbitAdmin(connections).getQueueProperties(settings.queue());
        return properties == null ? -1 : (Integer) properties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
    }
}