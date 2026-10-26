package com.sahil.jobplatform.messaging;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AttemptSupervisorTests {

    @Test
    void databaseUncertaintyCancelsHandlerAndNeverAuthorizesAnOutcomeWrite() throws Exception {
        var store = mock(JobExecutionStore.class);
        when(store.supervise(any(), any(), anyInt(), anyLong(), anyBoolean(), anyLong()))
                .thenThrow(new IllegalStateException("Database unavailable"));
        var settings = new ReliabilitySettings(50, 20, 10000, 1000, false);
        var interrupted = new java.util.concurrent.CountDownLatch(1);
        var finished = new java.util.concurrent.CountDownLatch(1);
        try (var controls = new ScheduledThreadPoolExecutor(1); var handlers = Executors.newSingleThreadExecutor()) {
            var supervisor = new AttemptSupervisor(store, settings, controls);
            var write = mock(java.util.function.BooleanSupplier.class);
            var work = handlers.submit(() -> {
                var claim = new JobExecutionStore.Claim(JobExecutionStore.Decision.CLAIMED,
                        UUID.randomUUID(), UUID.randomUUID(), "SIMULATED_WORK", null, 1);
                try (var watch = supervisor.watch(claim, 2)) {
                    try { finished.await(5, TimeUnit.SECONDS); }
                    catch (InterruptedException cancelled) { interrupted.countDown(); }
                    assertThatThrownBy(() -> watch.finish(write)).hasMessageContaining("reconciliation");
                }
            });
            assertThat(interrupted.await(3, TimeUnit.SECONDS)).isTrue();
            finished.countDown();
            work.get(5, TimeUnit.SECONDS);
            verifyNoInteractions(write);
        }
    }

    @Test
    void registrationRejectedDuringExecutorShutdownLeavesNoActiveWatch() {
        var store = mock(JobExecutionStore.class);
        try (var controls = new ScheduledThreadPoolExecutor(1)) {
            var supervisor = new AttemptSupervisor(store, new ReliabilitySettings(50, 20, 10000, 1000, false), controls);
            controls.shutdownNow();
            var claim = new JobExecutionStore.Claim(JobExecutionStore.Decision.CLAIMED,
                    UUID.randomUUID(), UUID.randomUUID(), "ECHO", null, 1);
            assertThatThrownBy(() -> supervisor.watch(claim, 2)).hasMessageContaining("shutting down");
            assertThat((java.util.Set<?>) org.springframework.test.util.ReflectionTestUtils.getField(supervisor, "active"))
                    .isEmpty();
            verifyNoInteractions(store);
        }
    }
}