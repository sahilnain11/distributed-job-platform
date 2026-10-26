package com.sahil.jobplatform.observability;

import com.sahil.jobplatform.persistence.JobStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/** Scrape reads cached scalars only: no database call occurs in a gauge callback. */
@Component
@ConditionalOnProperty(name = "platform.observability.snapshot-enabled", havingValue = "true", matchIfMissing = true)
public class DatabaseMetricsSnapshot {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseMetricsSnapshot.class);
    private final JdbcTemplate jdbc;
    private final MeterRegistry registry;
    private volatile Map<String, Double> values = Map.of();
    private volatile double lastSuccess;
    private volatile double up;

    public DatabaseMetricsSnapshot(DataSource source, MeterRegistry registry) {
        this.jdbc = new JdbcTemplate(source);
        this.jdbc.setQueryTimeout(2);
        this.registry = registry;
        for (var status : JobStatus.values()) {
            Gauge.builder("jobs.state", this, sample -> sample.value("state_" + status))
                    .tag("status", status.name()).register(registry);
        }
        gauge("jobs.outbox.pending", "outbox");
        gauge("jobs.retry.due", "due");
        gauge("jobs.leases.expired", "expired");
        gauge("jobs.outbox.oldest.age", "oldest");
        Gauge.builder("jobs.snapshot.up", this, sample -> sample.up).register(registry);
        Gauge.builder("jobs.snapshot.last.success", this, sample -> sample.lastSuccess).baseUnit("seconds").register(registry);
    }

    private void gauge(String metric, String key) {
        var builder = Gauge.builder(metric, this, sample -> sample.value(key));
        if (key.equals("oldest")) { builder.baseUnit("seconds"); }
        builder.register(registry);
    }

    private double value(String key) { return values.getOrDefault(key, Double.NaN); }

    @Scheduled(scheduler = "observabilityScheduler", fixedDelayString = "${platform.observability.snapshot-interval-ms:10000}",
            initialDelayString = "${platform.observability.snapshot-interval-ms:10000}")
    public void refresh() {
        try {
            Map<String, Double> snapshot = new HashMap<>();
            for (var status : JobStatus.values()) { snapshot.put("state_" + status, 0.0); }
            jdbc.query("""
                    SELECT 'state_' || status AS name, count(*)::double precision AS value
                    FROM job_platform.jobs GROUP BY status
                    UNION ALL SELECT 'outbox', count(*)::double precision FROM job_platform.outbox WHERE NOT published
                    UNION ALL SELECT 'due', count(*)::double precision FROM job_platform.jobs
                        WHERE status = 'RETRYING' AND next_retry_at <= clock_timestamp()
                    UNION ALL SELECT 'expired', count(*)::double precision FROM job_platform.jobs
                        WHERE status = 'RUNNING' AND lease_expiry <= clock_timestamp()
                    UNION ALL SELECT 'oldest', coalesce(extract(epoch FROM (clock_timestamp() - min(created_at))), 0)::double precision
                        FROM job_platform.outbox WHERE NOT published
                    """, row -> { snapshot.put(row.getString("name"), row.getDouble("value")); });
            values = Map.copyOf(snapshot);
            lastSuccess = System.currentTimeMillis() / 1000.0;
            up = 1;
        } catch (RuntimeException failure) {
            up = 0; // Keep last-known values; freshness/up explicitly marks failure.
            try { registry.counter("jobs.snapshot.failures").increment(); }
            catch (RuntimeException ignored) { /* Exporter failure must not stop future snapshots. */ }
            LOG.warn("Database metrics snapshot unavailable: {}", failure.getClass().getSimpleName());
        }
    }
}