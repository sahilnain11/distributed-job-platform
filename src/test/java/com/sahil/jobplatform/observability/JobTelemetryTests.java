package com.sahil.jobplatform.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class JobTelemetryTests {

    @Test
    void initialSnapshotIsUnknownAndGaugeReadsNeverContactDatabase() {
        var registry = new SimpleMeterRegistry();
        try {
            var source = mock(javax.sql.DataSource.class);
            var snapshot = new DatabaseMetricsSnapshot(source, registry);
            assertThat(registry.get("jobs.state").tag("status", "QUEUED").gauge().value()).isNaN();
            assertThat(registry.get("jobs.snapshot.up").gauge().value()).isZero();
            assertThat(registry.get("jobs.snapshot.last.success").gauge().value()).isZero();
            registry.getMeters().forEach(meter -> meter.measure().forEach(measurement -> measurement.getValue()));
            verifyNoInteractions(source);
            assertThat(snapshot).isNotNull(); // Keep a strong reference to gauge owner.
        } finally { registry.close(); }
    }

    @Test
    void dynamicInputsCannotCreateUnboundedLabelValues() {
        var registry = new SimpleMeterRegistry();
        try {
            var telemetry = new JobTelemetry(registry);
            for (int i = 0; i < 20; i++) {
                telemetry.created(UUID.randomUUID(), "unknown-" + UUID.randomUUID());
                telemetry.request("RETRY_JOB:" + UUID.randomUUID(), false, 9000 + i);
            }
            assertThat(registry.get("jobs.accepted").tag("type", "OTHER").counter().count()).isEqualTo(20);
            assertThat(registry.find("jobs.accepted").counters()).hasSize(1);
            assertThat(registry.get("jobs.api.outcomes").tags("operation", "OTHER", "status", "OTHER").counter().count()).isEqualTo(20);
            assertThat(registry.find("jobs.api.outcomes").counters()).hasSize(1);
        } finally { registry.close(); }
    }

    @Test
    void brokenMetricExporterCannotEscapeCommitCallbacksOrChangeBusinessOutcome() {
        MeterRegistry registry = mock(MeterRegistry.class, RETURNS_DEEP_STUBS);
        JobTelemetry telemetry = new JobTelemetry(registry);
        when(registry.counter(anyString(), any(String[].class))).thenThrow(new IllegalStateException("Exporter unavailable"));
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            telemetry.created(UUID.randomUUID(), "ECHO");
            for (var callback : TransactionSynchronizationManager.getSynchronizations()) {
                assertThatCode(callback::afterCommit).doesNotThrowAnyException();
            }
            assertThatCode(() -> telemetry.request("SUBMIT_JOB", false, 202)).doesNotThrowAnyException();
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }
}