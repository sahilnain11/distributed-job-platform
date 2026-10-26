package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real service outages, never faults against development Compose or replacement containers. */
@Testcontainers
@Execution(ExecutionMode.SAME_THREAD)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "platform.messaging.enabled=true", "management.health.rabbit.enabled=true",
        "platform.messaging.worker-concurrency=2", "platform.messaging.lease-seconds=2",
        "platform.messaging.publisher-delay-ms=100", "platform.messaging.confirm-timeout-ms=500",
        "platform.reliability.heartbeat-ms=150", "platform.reliability.control-interval-ms=50",
        "platform.reliability.max-runtime-ms=20000", "platform.reliability.retry-base-ms=100",
        "platform.reliability.retry-poll-ms=100", "platform.reliability.recovery-poll-ms=100",
        "spring.datasource.hikari.connection-timeout=500", "spring.datasource.hikari.validation-timeout=250",
        "spring.datasource.hikari.maximum-pool-size=4", "spring.datasource.hikari.minimum-idle=0",
        "spring.jdbc.template.query-timeout=2s", "spring.rabbitmq.connection-timeout=500ms",
        "platform.observability.snapshot-enabled=false"})
class DependencyOutageIntegrationTests {

    private static final Duration RECOVERY = Duration.ofSeconds(45);
    private static final Duration LEASE = Duration.ofSeconds(2);
    private static final String PG_DATA = "/var/lib/postgresql/data";

    @Container @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6")
            // Normally postgres is PID 1 and pg_ctl stop would exit its container.
            // Keep only this disposable container alive while its server is down;
            // the official entrypoint still initializes the real PostgreSQL server.
            .withCommand("sh", "-c", "docker-entrypoint.sh postgres & exec tail -f /dev/null")
            .withUrlParam("connectTimeout", "2").withUrlParam("socketTimeout", "2");

    @Container @ServiceConnection
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1.8-management");

    @Autowired private TestRestTemplate http;
    @Autowired private ObjectMapper mapper;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private MeterRegistry metrics;
    @Autowired private CachingConnectionFactory connections;
    @Autowired private SimpleMessageListenerContainer consumer;
    @Autowired private MessagingSettings settings;
    @Autowired private WorkerBackoff backoff;
    @Autowired private JobExecutionStore store;

    private boolean postgresNeedsRestore;
    private boolean rabbitNeedsRestore;

    @BeforeEach
    void awaitPreviousWorkWithoutDeletingAnyData() {
        var requests = new SimpleClientHttpRequestFactory();
        requests.setConnectTimeout(2000);
        requests.setReadTimeout(10000);
        http.getRestTemplate().setRequestFactory(requests);
        await().atMost(RECOVERY).untilAsserted(() -> {
            assertThat(count("SELECT count(*) FROM jobs WHERE status <> 'COMPLETED'")).isZero();
            assertThat(activeHandlers()).isZero();
            assertThat(backoff.isPaused()).isFalse();
            assertThat(consumer.isRunning()).isTrue();
        });
    }

    @Test
    void brokerOutageAcceptsAndReplaysWhileOutboxWaitsThenDrainsAlongsideDurableBacklog() throws Exception {
        String rabbitId = RABBIT.getContainerId();
        int rabbitPort = RABBIT.getAmqpPort();
        String key = UUID.randomUUID().toString();
        String body = echo(key);
        // Hold one confirmed persistent notification on the durable queue before
        // stopping the broker application, not merely a new unconfirmed send.
        consumer.stop();
        try {
            UUID backlog = acceptedId(post(UUID.randomUUID().toString(), echo("already-confirmed")));
            await().atMost(RECOVERY).untilAsserted(() -> {
                assertPublished(backlog, true);
                assertThat(queueDepth()).isEqualTo(1);
            });
            stopRabbit();
            long unavailableAt = System.nanoTime();
            assertThat(exec(RABBIT.getContainerId(), "rabbitmq", "rabbitmq-diagnostics", "-q", "check_running").code())
                    .as("Rabbit application really stopped").isNotZero();
            assertThatThrownBy(() -> {
                try (var ignored = connections.getRabbitConnectionFactory().newConnection()) {
                    // A fresh AMQP connection must fail, not reuse a cached handle.
                }
            }).isInstanceOfAny(IOException.class, TimeoutException.class);
            assertThat(http.getForEntity("/actuator/health/readiness", String.class).getStatusCode().value()).isEqualTo(200);
            // Start while the broker is still DOWN. From here on the listener
            // must reconnect by itself; there is no manual start after return.
            consumer.start();
            assertThat(consumer.isRunning()).isTrue();

            double failures = metrics.counter("jobs.outbox.failures").count();
            var accepted = post(key, body);
            UUID waiting = acceptedId(accepted);
            assertReplay(key, body, accepted);
            await().pollInterval(Duration.ofMillis(50)).during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        assertThat(metrics.counter("jobs.outbox.failures").count()).isGreaterThan(failures);
                        assertPublished(waiting, false);
                        assertThat(state(waiting)).isEqualTo("QUEUED");
                        assertThat(count("SELECT count(*) FROM job_attempts WHERE job_id = ?", waiting)).isZero();
                    });
            assertSavedRequest(key, accepted);

            restoreRabbit();
            long returnedAt = System.nanoTime();
            assertThat(RABBIT.getContainerId()).isEqualTo(rabbitId);
            assertThat(RABBIT.getAmqpPort()).isEqualTo(rabbitPort);
            awaitCompletedOnce(backlog);
            awaitCompletedOnce(waiting);
            await().atMost(RECOVERY).untilAsserted(() -> {
                assertThat(consumer.getActiveConsumerCount()).isEqualTo(2);
                assertThat(queueDepth()).isZero();
                assertThat(activeHandlers()).isZero();
            });
            assertReplay(key, body, accepted);
            assertThat(get(waiting).path("result").path("value").path("message").asText()).isEqualTo(key);
            evidence("broker-outage", unavailableAt, returnedAt,
                    mapper.createObjectNode().put("outageHttpStatus", 202).put("unpublishedWhileDown", true)
                            .put("durableBacklogSurvived", true).put("automaticConsumerReconnect", true).put("completedJobs", 2));
        } finally {
            try { restoreRabbit(); }
            finally { if (!consumer.isRunning()) { consumer.start(); } }
        }
    }

    @Test
    void databaseOutageReturns503ThenSameKeyCommitsOnceAndKeepsItsSavedReplay() throws Exception {
        UUID durableJob = acceptedId(post(UUID.randomUUID().toString(), echo("before-database-outage")));
        awaitCompletedOnce(durableJob);
        var identity = postgresIdentity();
        int jobsBefore = count("SELECT count(*) FROM jobs");
        String key = UUID.randomUUID().toString();
        String body = echo(key);
        long unavailableAt;
        try {
            stopPostgres();
            unavailableAt = System.nanoTime();
            var rejected = post(key, body);
            assertThat(rejected.getStatusCode().value()).as("No 202 without a database commit").isEqualTo(503);
            assertThat(mapper.readTree(rejected.getBody()).path("error").path("code").asText()).isEqualTo("STORAGE_UNAVAILABLE");
            assertThat(http.getForEntity("/jobs/" + durableJob, String.class).getStatusCode().value()).isEqualTo(503);
            assertThat(http.getForEntity("/actuator/health/readiness", String.class).getStatusCode().value()).isEqualTo(503);
        } finally {
            restorePostgres();
        }
        long returnedAt = System.nanoTime();
        assertThat(postgresIdentity()).as("Same container, port, JDBC URL and PostgreSQL cluster").isEqualTo(identity);
        assertThat(count("SELECT count(*) FROM jobs")).isEqualTo(jobsBefore);
        assertThat(count("SELECT count(*) FROM idempotency_keys WHERE idempotency_key = ?", key)).isZero();
        assertThat(get(durableJob).path("status").asText()).isEqualTo("COMPLETED");

        var accepted = post(key, body);
        UUID id = acceptedId(accepted);
        assertReplay(key, body, accepted);
        awaitCompletedOnce(id);
        assertReplay(key, body, accepted);
        assertSavedRequest(key, accepted);
        assertThat(count("SELECT count(*) FROM jobs")).isEqualTo(jobsBefore + 1);
        assertThat(count("SELECT count(*) FROM job_payloads WHERE job_id = ?", id)).isEqualTo(1);
        assertThat(get(id).path("result").path("value").path("message").asText()).isEqualTo(key);
        assertThat(post(key, echo("changed-request")).getStatusCode().value()).isEqualTo(409);
        evidence("database-acceptance-outage", unavailableAt, returnedAt,
                mapper.createObjectNode().put("outageHttpStatus", 503).put("restoredHttpStatus", 202)
                        .put("savedRequests", 1).put("committedJobs", 1).put("replayStable", true));
    }

    @Test
    void databaseOutageCancelsActiveSimulationThenRecoversExpiredAttemptAndRejectsStaleWrites() throws Exception {
        String key = UUID.randomUUID().toString();
        String body = "{\"type\":\"SIMULATED_WORK\",\"payload\":{\"durationMillis\":6000,\"failFirstAttempts\":0}}";
        var accepted = post(key, body);
        UUID id = acceptedId(accepted);
        await().atMost(RECOVERY).untilAsserted(() -> {
            assertThat(state(id)).isEqualTo("RUNNING");
            assertThat(activeHandlers()).isEqualTo(1);
            // Prove the real supervisor is renewing this attempt before the fault.
            assertThat(jdbc.queryForObject("SELECT lease_expiry > started_at + interval '2 seconds' FROM jobs WHERE job_id = ?",
                    Boolean.class, id)).isTrue();
        });
        UUID abandonedAttempt = jdbc.queryForObject("SELECT attempt_id FROM jobs WHERE job_id = ?", UUID.class, id);
        var identity = postgresIdentity();
        long unavailableAt;
        try {
            stopPostgres();
            unavailableAt = System.nanoTime();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(backoff.isPaused()).as("DB uncertainty pauses intake rather than ACKing a guessed outcome").isTrue();
                assertThat(activeHandlers()).as("Cooperative simulation actually relinquishes its slot").isZero();
            });
            // Synchronize on a full lease after confirmed server shutdown. No
            // timestamp fixtures or sleeps: even the final pre-stop renewal must expire.
            await().atMost(Duration.ofSeconds(5)).until(() -> System.nanoTime() - unavailableAt >= LEASE.toNanos());
            assertThat(exec(POSTGRES.getContainerId(), "postgres", "pg_isready", "-h", "127.0.0.1").code()).isEqualTo(2);
        } finally {
            restorePostgres();
        }
        long returnedAt = System.nanoTime();
        assertThat(postgresIdentity()).isEqualTo(identity);
        await().atMost(RECOVERY).untilAsserted(() -> {
            assertThat(state(id)).isEqualTo("RUNNING");
            assertThat(count("SELECT attempt_count FROM jobs WHERE job_id = ?", id)).isEqualTo(2);
            assertThat(jdbc.queryForObject("SELECT status FROM job_attempts WHERE attempt_id = ?", String.class, abandonedAttempt))
                    .isEqualTo("EXPIRED");
        });
        UUID replacement = jdbc.queryForObject("SELECT attempt_id FROM jobs WHERE job_id = ?", UUID.class, id);
        assertThat(replacement).isNotEqualTo(abandonedAttempt);
        assertThat(count("SELECT count(*) FROM job_results WHERE job_id = ?", id)).isZero();
        assertThat(store.complete(id, abandonedAttempt, mapper.createObjectNode().put("message", "stale-poison"))).isFalse();
        assertThat(store.failRetryably(id, abandonedAttempt, "SIMULATED_FAILURE", "stale failure", 100)).isFalse();

        awaitState(id, "COMPLETED");
        var job = get(id);
        assertThat(jdbc.queryForList("SELECT status FROM job_attempts WHERE job_id = ? ORDER BY attempt_number", String.class, id))
                .containsExactly("EXPIRED", "COMPLETED");
        assertThat(jdbc.queryForList("SELECT attempt_number FROM job_attempts WHERE job_id = ? ORDER BY attempt_number", Integer.class, id))
                .containsExactly(1, 2);
        assertThat(job.path("attemptCount").asInt()).isEqualTo(2);
        assertThat(job.path("cycleAttemptCount").asInt()).isEqualTo(2);
        assertThat(job.path("attempts").get(0).path("failureType").asText()).isEqualTo("LEASE_EXPIRED");
        assertThat(job.path("attempts").get(0).path("endedAt").isNull()).isFalse();
        assertThat(job.path("result").path("value").path("cycleAttempt").asInt()).isEqualTo(2);
        assertThat(job.path("result").path("value").path("message").asText()).isEqualTo("Simulated work completed");
        assertThat(count("SELECT count(*) FROM job_results WHERE job_id = ?", id)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox WHERE job_id = ?", id)).isEqualTo(2);
        await().atMost(RECOVERY).untilAsserted(() -> {
            assertThat(count("SELECT count(*) FROM outbox WHERE job_id = ? AND NOT published", id)).isZero();
            assertThat(activeHandlers()).isZero();
            assertThat(backoff.isPaused()).isFalse();
            assertThat(queueDepth()).isZero();
        });
        assertThat(store.complete(id, abandonedAttempt, mapper.createObjectNode().put("message", "late-poison"))).isFalse();
        assertThat(get(id).path("result")).isEqualTo(job.path("result"));
        assertReplay(key, body, accepted);
        assertSavedRequest(key, accepted);
        evidence("database-active-attempt-outage", unavailableAt, returnedAt,
                mapper.createObjectNode().put("supervisionLost", true).put("cooperativeCancellationObserved", true)
                        .put("expiredAttempts", 1).put("completedAttempts", 1).put("results", 1).put("staleWritesRejected", true));
    }

    private void stopPostgres() throws Exception {
        postgresNeedsRestore = true; // Also restore if a control command/assertion fails partway through.
        assertSuccess(exec(POSTGRES.getContainerId(), "postgres", "pg_ctl", "-D", PG_DATA,
                "-m", "fast", "-w", "-t", "15", "stop"));
        assertThat(POSTGRES.isRunning()).as("Only the PostgreSQL server, not its container, was stopped").isTrue();
        assertThat(exec(POSTGRES.getContainerId(), "postgres", "pg_ctl", "-D", PG_DATA, "status").code()).isEqualTo(3);
        assertThat(exec(POSTGRES.getContainerId(), "postgres", "pg_isready", "-h", "127.0.0.1").code()).isEqualTo(2);
    }

    private void restorePostgres() throws Exception {
        if (!postgresNeedsRestore) { return; }
        if (exec(POSTGRES.getContainerId(), "postgres", "pg_ctl", "-D", PG_DATA, "status").code() != 0) {
            // A file log avoids inheriting Docker exec's stdout in the daemon.
            assertSuccess(exec(POSTGRES.getContainerId(), "postgres", "pg_ctl", "-D", PG_DATA,
                    "-l", "/tmp/dependency-outage-postgres.log", "-w", "-t", "15", "start"));
        }
        await().atMost(RECOVERY).ignoreExceptionsMatching(failure -> failure instanceof DataAccessException)
                .untilAsserted(() -> {
                    assertThat(jdbc.queryForObject("SELECT 1", Integer.class)).isEqualTo(1);
                    assertThat(http.getForEntity("/actuator/health/readiness", String.class).getStatusCode().value()).isEqualTo(200);
                });
        postgresNeedsRestore = false;
    }

    private void stopRabbit() throws Exception {
        rabbitNeedsRestore = true;
        assertSuccess(exec(RABBIT.getContainerId(), "rabbitmq", "rabbitmqctl", "-t", "15", "stop_app"));
        assertThat(RABBIT.isRunning()).as("Only Rabbit's application, not its container, was stopped").isTrue();
    }

    private void restoreRabbit() throws Exception {
        if (!rabbitNeedsRestore) { return; }
        assertSuccess(exec(RABBIT.getContainerId(), "rabbitmq", "rabbitmqctl", "-t", "15", "start_app"));
        await().atMost(RECOVERY).ignoreExceptionsMatching(failure -> failure instanceof org.springframework.amqp.AmqpException)
                .untilAsserted(() -> assertThat(queueDepth()).isGreaterThanOrEqualTo(0));
        rabbitNeedsRestore = false;
    }

    /** Every exec is bounded, uses only this class's disposable container IDs, and specifies the OS user. */
    private ExecResult exec(String id, String user, String... command) throws Exception {
        var docker = DockerClientFactory.instance().client();
        var created = docker.execCreateCmd(id).withUser(user).withAttachStdout(true).withAttachStderr(true).withCmd(command).exec();
        var stdout = new ByteArrayOutputStream();
        var stderr = new ByteArrayOutputStream();
        try (var callback = new ResultCallback.Adapter<Frame>() {
            @Override public void onNext(Frame frame) {
                (frame.getStreamType() == StreamType.STDERR ? stderr : stdout).writeBytes(frame.getPayload());
            }
        }) {
            docker.execStartCmd(created.getId()).exec(callback);
            assertThat(callback.awaitCompletion(25, TimeUnit.SECONDS)).as("Bounded dependency command: %s", String.join(" ", command)).isTrue();
        }
        Long code = docker.inspectExecCmd(created.getId()).exec().getExitCodeLong();
        assertThat(code).isNotNull();
        return new ExecResult(code, stdout.toString(StandardCharsets.UTF_8), stderr.toString(StandardCharsets.UTF_8));
    }

    private void assertSuccess(ExecResult result) {
        assertThat(result.code()).as("Dependency control stdout: %s; stderr: %s", result.stdout(), result.stderr()).isZero();
    }

    private PostgresIdentity postgresIdentity() {
        return new PostgresIdentity(POSTGRES.getContainerId(), POSTGRES.getMappedPort(5432), POSTGRES.getJdbcUrl(),
                jdbc.queryForObject("SELECT system_identifier::text FROM pg_control_system()", String.class));
    }

    private ResponseEntity<String> post(String key, String body) {
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", key);
        return http.postForEntity("/jobs", new HttpEntity<>(body, headers), String.class);
    }

    private String echo(String message) {
        return mapper.createObjectNode().put("type", "ECHO")
                .set("payload", mapper.createObjectNode().put("message", message)).toString();
    }

    private UUID acceptedId(ResponseEntity<String> response) throws Exception {
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        return UUID.fromString(mapper.readTree(response.getBody()).path("jobId").asText());
    }

    private void assertReplay(String key, String body, ResponseEntity<String> original) throws Exception {
        var replay = post(key, body);
        assertThat(replay.getStatusCode()).isEqualTo(original.getStatusCode());
        // JSONB does not promise byte-for-byte object-field order.
        assertThat(mapper.readTree(replay.getBody())).isEqualTo(mapper.readTree(original.getBody()));
    }

    private void assertSavedRequest(String key, ResponseEntity<String> accepted) throws Exception {
        assertThat(count("SELECT count(*) FROM idempotency_keys WHERE idempotency_key = ?", key)).isEqualTo(1);
        assertThat(count("SELECT response_status FROM idempotency_keys WHERE idempotency_key = ?", key)).isEqualTo(202);
        assertThat(mapper.readTree(jdbc.queryForObject("SELECT response_body::text FROM idempotency_keys WHERE idempotency_key = ?",
                String.class, key))).isEqualTo(mapper.readTree(accepted.getBody()));
    }

    private void awaitCompletedOnce(UUID id) {
        awaitState(id, "COMPLETED");
        await().atMost(RECOVERY).untilAsserted(() -> assertPublished(id, true));
        assertThat(count("SELECT attempt_count FROM jobs WHERE job_id = ?", id)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM job_attempts WHERE job_id = ?", id)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM job_results WHERE job_id = ?", id)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox WHERE job_id = ?", id)).isEqualTo(1);
    }

    private void assertPublished(UUID id, boolean expected) {
        assertThat(jdbc.queryForObject("SELECT published FROM outbox WHERE job_id = ?", Boolean.class, id)).isEqualTo(expected);
        assertThat(jdbc.queryForObject("SELECT published_at IS NOT NULL FROM outbox WHERE job_id = ?", Boolean.class, id)).isEqualTo(expected);
    }

    private int count(String sql, Object... arguments) { return jdbc.queryForObject(sql, Integer.class, arguments); }
    private String state(UUID id) { return jdbc.queryForObject("SELECT status FROM jobs WHERE job_id = ?", String.class, id); }
    private double activeHandlers() { return metrics.get("jobs.worker.active").gauge().value(); }

    private int queueDepth() {
        var properties = new RabbitAdmin(connections).getQueueProperties(settings.queue());
        return properties == null ? -1 : (Integer) properties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
    }

    private void awaitState(UUID id, String state) {
        await().atMost(RECOVERY).untilAsserted(() -> assertThat(state(id)).isEqualTo(state));
    }

    private JsonNode get(UUID id) throws Exception {
        var response = http.getForEntity("/jobs/" + id, String.class);
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        return mapper.readTree(response.getBody());
    }

    private void evidence(String scenario, long unavailableAt, long returnedAt, JsonNode facts) throws Exception {
        System.out.println(mapper.writeValueAsString(mapper.createObjectNode().put("event", "dependency_outage_experiment")
                .put("scenario", scenario).put("postgres", "17.6").put("rabbitmq", "4.1.8")
                .put("leaseMs", LEASE.toMillis()).put("heartbeatMs", 150).put("pollMs", 100)
                .put("confirmedOutageToReturnMs", TimeUnit.NANOSECONDS.toMillis(returnedAt - unavailableAt))
                .put("returnToAssertionsCompleteMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - returnedAt))
                .set("evidence", facts)));
    }

    private record ExecResult(long code, String stdout, String stderr) { }
    private record PostgresIdentity(String containerId, int port, String jdbcUrl, String systemIdentifier) { }
}