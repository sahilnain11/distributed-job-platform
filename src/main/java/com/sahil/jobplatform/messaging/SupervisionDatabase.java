package com.sahil.jobplatform.messaging;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Objects;
import java.util.function.Function;

/** Small independent pool: handler/API pool saturation must not starve leases. */
@Component
public class SupervisionDatabase implements AutoCloseable {

    private final HikariDataSource dataSource;
    private final TransactionTemplate transaction;
    private final JdbcTemplate jdbc;

    public SupervisionDatabase(JdbcConnectionDetails connection, io.micrometer.core.instrument.MeterRegistry metrics) {
        var config = new HikariConfig();
        config.setPoolName("attempt-control");
        config.setMetricsTrackerFactory(new com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory(metrics));
        config.setJdbcUrl(connection.getJdbcUrl());
        config.setUsername(connection.getUsername());
        config.setPassword(connection.getPassword());
        config.setDriverClassName(connection.getDriverClassName());
        config.setSchema("job_platform");
        config.setMaximumPoolSize(4);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(1000);
        config.setInitializationFailTimeout(-1);
        // Not exposed as a DataSource bean: Boot still owns the primary JPA pool.
        dataSource = new HikariDataSource(config);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.setQueryTimeout(2);
        transaction = new TransactionTemplate(new JdbcTransactionManager(dataSource));
        transaction.setTimeout(3);
    }

    public <T> T execute(Function<JdbcTemplate, T> operation) {
        return Objects.requireNonNull(transaction.execute(status -> operation.apply(jdbc)));
    }

    @Override
    @jakarta.annotation.PreDestroy
    public void close() {
        dataSource.close();
    }
}