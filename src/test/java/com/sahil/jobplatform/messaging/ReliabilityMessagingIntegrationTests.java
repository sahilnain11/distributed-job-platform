package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sahil.jobplatform.persistence.JobPersistenceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "platform.messaging.enabled=true", "management.health.rabbit.enabled=true",
        "platform.messaging.worker-concurrency=2", "platform.messaging.lease-seconds=2",
        "platform.messaging.publisher-delay-ms=50", "platform.reliability.heartbeat-ms=150",
        "platform.reliability.control-interval-ms=50", "platform.reliability.max-runtime-ms=5000",
        "platform.reliability.retry-base-ms=50", "platform.reliability.retry-poll-ms=50",
        "platform.reliability.recovery-poll-ms=100"})
class ReliabilityMessagingIntegrationTests {

    @Container @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");
    @Container @ServiceConnection
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1.8-management");

    @Autowired private TestRestTemplate http;
    @Autowired private ObjectMapper mapper;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private JobExecutionStore store;
    @Autowired private JobPersistenceService persistence;
    @Autowired private PlatformTransactionManager transactions;
    @Autowired private org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer container;

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE idempotency_keys, outbox, job_results, job_attempts, job_payloads, jobs");
    }

    @Test
    void simulationFailsTwiceThenCompletesThroughSchedulerOutboxAndRealBroker() throws Exception {
        UUID id = submit(20, 2);
        awaitState(id, "COMPLETED");
        assertThat(jdbc.queryForList("SELECT status FROM job_attempts WHERE job_id = ? ORDER BY attempt_number", String.class, id))
                .containsExactly("FAILED", "FAILED", "COMPLETED");
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM jobs WHERE job_id = ?", Integer.class, id)).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, id)).isEqualTo(3);
        JsonNode job = get(id);
        assertThat(job.path("failure").isNull()).isTrue();
        assertThat(job.path("result").path("value").path("cycleAttempt").asInt()).isEqualTo(3);
        assertThat(job.path("attempts").size()).isEqualTo(3);
    }

    @Test
    void exhaustedJobCanRunMultipleManualCyclesWithMonotonicHistoryAndFrozenOldKey() throws Exception {
        UUID id = submit(0, 4);
        awaitState(id, "FAILED");
        assertThat(get(id).path("attemptCount").asInt()).isEqualTo(4);
        var headers = headers("manual-full-cycle");
        var retry = http.postForEntity("/jobs/" + id + "/retry", new HttpEntity<>("{}", headers), String.class);
        assertThat(retry.getStatusCode().value()).isEqualTo(202);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            var job = get(id);
            assertThat(job.path("status").asText()).isEqualTo("FAILED");
            assertThat(job.path("attemptCount").asInt()).isEqualTo(8);
        });
        var replay = http.postForEntity("/jobs/" + id + "/retry", new HttpEntity<>("{}", headers), String.class);
        assertThat(replay.getStatusCode().value()).isEqualTo(202);
        assertThat(mapper.readTree(replay.getBody())).isEqualTo(mapper.readTree(retry.getBody()));
        assertThat(get(id).path("status").asText()).isEqualTo("FAILED");
        assertThat(jdbc.queryForList("SELECT attempt_number FROM job_attempts WHERE job_id = ? ORDER BY attempt_number", Integer.class, id))
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE job_id = ?", Integer.class, id)).isEqualTo(8);
    }

    @Test
    void heartbeatKeepsWorkAliveBeyondItsInitialLeaseWithoutASecondAttempt() throws Exception {
        UUID id = submit(3200, 0);
        awaitState(id, "RUNNING");
        OffsetDateTime initialDeadline = jdbc.queryForObject("SELECT started_at + interval '2 seconds' FROM jobs WHERE job_id = ?",
                OffsetDateTime.class, id);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            OffsetDateTime now = jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class);
            assertThat(now).isAfter(initialDeadline);
            assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RUNNING");
            assertThat(jdbc.queryForObject("SELECT lease_expiry FROM jobs WHERE job_id = ?", OffsetDateTime.class, id)).isAfter(now);
        });
        awaitState(id, "COMPLETED");
        assertThat(get(id).path("attemptCount").asInt()).isEqualTo(1);
    }

    @Test
    void timeoutCancellationRetriesAndEventuallyExhaustsWithoutSavingAResult() throws Exception {
        UUID id = submit(20000, 0);
        // Advance only test attempt start times, not a production clock. Every
        // actual watchdog invocation still checks DB time/ownership and interrupts
        // the cancellable handler; avoids waiting four production five-minute limits.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            jdbc.update("""
                    UPDATE job_attempts SET started_at = clock_timestamp() - interval '6 seconds'
                    WHERE job_id = ? AND status = 'RUNNING'
                    """, id);
            assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("FAILED");
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_attempts WHERE job_id = ? AND failure_type = 'TIMEOUT'",
                Integer.class, id)).isEqualTo(4);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_results WHERE job_id = ?", Integer.class, id)).isZero();
        assertThat(get(id).path("cycleAttemptCount").asInt()).isEqualTo(4);
    }

    @Test
    void anActualWorkerProcessKilledAfterClaimIsRecoveredByTheRemainingPlatform() throws Exception {
        container.stop();
        Process child = null;
        java.nio.file.Path log = java.nio.file.Files.createTempFile("job-worker-crash-test-", ".log");
        try {
            String javaExecutable = java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString();
            child = new ProcessBuilder(javaExecutable, "-cp",
                    System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    "com.sahil.jobplatform.JobPlatformApplication",
                    "--spring.main.web-application-type=none",
                    "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                    "--spring.datasource.username=" + POSTGRES.getUsername(),
                    "--spring.datasource.password=" + POSTGRES.getPassword(),
                    "--spring.rabbitmq.host=" + RABBIT.getHost(),
                    "--spring.rabbitmq.port=" + RABBIT.getAmqpPort(),
                    "--spring.rabbitmq.username=" + RABBIT.getAdminUsername(),
                    "--spring.rabbitmq.password=" + RABBIT.getAdminPassword(),
                    "--platform.messaging.enabled=true", "--platform.messaging.publisher-enabled=false",
                    "--platform.messaging.worker-enabled=true", "--platform.messaging.worker-concurrency=1",
                    "--platform.messaging.lease-seconds=2", "--platform.reliability.heartbeat-ms=150",
                    "--platform.reliability.control-interval-ms=50", "--platform.reliability.pollers-enabled=false")
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            UUID id = submit(1200, 0);
            Process workerProcess = child;
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(workerProcess.isAlive()).as("Worker child startup; log: %s", log).isTrue();
                assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo("RUNNING");
            });
            child.destroyForcibly();
            assertThat(child.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            container.start();
            awaitState(id, "COMPLETED");
            assertThat(jdbc.queryForList("SELECT status FROM job_attempts WHERE job_id = ? ORDER BY attempt_number", String.class, id))
                    .containsExactly("EXPIRED", "COMPLETED");
            assertThat(get(id).path("attemptCount").asInt()).isEqualTo(2);
        } finally {
            if (child != null && child.isAlive()) { child.destroyForcibly(); child.waitFor(10, java.util.concurrent.TimeUnit.SECONDS); }
            if (!container.isRunning()) { container.start(); }
        }
    }

    @Test
    void abandonedClaimIsRecoveredAndOldOwnerCannotOverwriteTheReplacement() throws Exception {
        UUID id = UUID.randomUUID();
        var abandoned = new TransactionTemplate(transactions).execute(tx -> {
            persistence.createJob(id, "ECHO", mapper.createObjectNode().put("message", "recover abandoned claim"));
            return store.claim(id, UUID.randomUUID(), 2);
        });
        // No worker supervision is registered for this controlled crash-after-
        // committed-claim fixture. Recovery must progress without its old message.
        jdbc.update("UPDATE jobs SET lease_expiry = clock_timestamp() - interval '1 second' WHERE job_id = ?", id);
        awaitState(id, "COMPLETED");
        assertThat(jdbc.queryForList("SELECT status FROM job_attempts WHERE job_id = ? ORDER BY attempt_number", String.class, id))
                .containsExactly("EXPIRED", "COMPLETED");
        assertThat(store.complete(id, abandoned.attemptId(), mapper.createObjectNode().put("message", "stale"))).isFalse();
        assertThat(get(id).path("result").path("value").path("message").asText()).isEqualTo("recover abandoned claim");
    }

    private UUID submit(long duration, int failFirstAttempts) throws Exception {
        String body = "{\"type\":\"SIMULATED_WORK\",\"payload\":{\"durationMillis\":" + duration
                + ",\"failFirstAttempts\":" + failFirstAttempts + "}}";
        var response = http.postForEntity("/jobs", new HttpEntity<>(body, headers(UUID.randomUUID().toString())), String.class);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        return UUID.fromString(mapper.readTree(response.getBody()).path("jobId").asText());
    }

    private HttpHeaders headers(String key) {
        var headers = new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", key); return headers;
    }

    private void awaitState(UUID id, String state) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id)).isEqualTo(state));
    }

    private JsonNode get(UUID id) throws Exception {
        return mapper.readTree(http.getForEntity("/jobs/" + id, String.class).getBody());
    }
}