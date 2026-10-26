package com.sahil.jobplatform.persistence;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class MigrationUpgradeTests {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    @Test
    void upgradesAnExistingV1DatabaseWithoutChangingItsAppliedMigrationOrOtherData() {
        Flyway baseline = migration("1");
        baseline.migrate();
        JdbcTemplate jdbc = new JdbcTemplate(new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        var appliedV1 = jdbc.queryForMap("""
                SELECT checksum, installed_on FROM job_platform.flyway_schema_history WHERE version = '1'
                """);
        jdbc.execute("CREATE TABLE public.upgrade_sentinel (id INTEGER PRIMARY KEY)");
        jdbc.update("INSERT INTO public.upgrade_sentinel VALUES (42)");

        Flyway current = migration("latest");
        assertThat(current.migrate().migrationsExecuted).isEqualTo(1);

        assertThat(current.info().current().getVersion().getVersion()).isEqualTo("2");
        assertThat(current.validateWithResult().validationSuccessful).isTrue();
        assertThat(jdbc.queryForMap("""
                SELECT checksum, installed_on FROM job_platform.flyway_schema_history WHERE version = '1'
                """)).isEqualTo(appliedV1);
        assertThat(jdbc.queryForObject("SELECT id FROM public.upgrade_sentinel", Integer.class)).isEqualTo(42);
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = 'job_platform' AND table_name <> 'flyway_schema_history'
                """, Integer.class)).isEqualTo(6);
        assertThat(current.migrate().migrationsExecuted).isZero();
    }

    private Flyway migration(String target) {
        return Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .defaultSchema("job_platform")
                .schemas("job_platform")
                .createSchemas(true)
                .cleanDisabled(true)
                .target(target)
                .load();
    }
}