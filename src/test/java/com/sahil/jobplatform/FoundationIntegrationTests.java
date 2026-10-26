package com.sahil.jobplatform;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class FoundationIntegrationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:17.6"));

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private Flyway flyway;

    @Autowired
    private JdbcTemplate jdbc;

    @PersistenceContext
    private EntityManager entityManager;

    @Test
    void healthEndpointIsUpWithoutExposingConnectionDetails() {
        var response = http.getForEntity("/actuator/health", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"UP\"")
                .doesNotContain("jdbc:", "password", "components");
    }

    @Test
    void livenessAndDatabaseReadinessAreUp() {
        for (String probe : new String[]{"liveness", "readiness"}) {
            var response = http.getForEntity("/actuator/health/" + probe, String.class);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).contains("\"status\":\"UP\"");
        }
    }

    @Test
    void flywayAppliesAndValidatesTheFoundationMigration() {
        assertThat(flyway.validateWithResult().validationSuccessful).isTrue();
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("2");
        assertThat(jdbc.queryForObject("""
                SELECT obj_description(oid, 'pg_namespace')
                FROM pg_namespace WHERE nspname = 'job_platform'
                """, String.class)).isEqualTo("Distributed job platform application data");
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM job_platform.flyway_schema_history
                WHERE version = '1' AND success = true
                """, Integer.class)).isEqualTo(1);
    }

    @Test
    @Transactional(readOnly = true)
    void jpaConnectsToRealPostgreSqlInTheApplicationSchema() {
        assertThat(entityManager.createNativeQuery("SELECT current_schema()")
                .getSingleResult()).isEqualTo("job_platform");
        assertThat(jdbc.queryForObject("SELECT version()", String.class))
                .startsWith("PostgreSQL 17.6");
    }

    @Test
    void retryEndpointRequiresPostRatherThanChangingStateOnGet() {
        assertThat(http.getForEntity("/jobs/" + java.util.UUID.randomUUID() + "/retry", String.class).getStatusCode())
                .isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
    }
}