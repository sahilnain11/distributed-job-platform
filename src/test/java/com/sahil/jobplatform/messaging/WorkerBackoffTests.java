package com.sahil.jobplatform.messaging;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.StaticApplicationContext;

import java.time.Duration;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class WorkerBackoffTests {

    @Test
    void infrastructurePauseStopsOnceAndRestartsAfterBackoff() {
        var container = mock(SimpleMessageListenerContainer.class);
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            var backoff = new WorkerBackoff(provider(container), executor);
            backoff.pause(true);
            backoff.pause(true);
            assertThat(backoff.isPaused()).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(container).start());
            verify(container, times(1)).stop();
            assertThat(backoff.isPaused()).isFalse();
        }
    }

    @Test
    void deferredWorkStopsIntakeWithoutAutomaticallyRestartingUnsupportedExecution() {
        var container = mock(SimpleMessageListenerContainer.class);
        try (var executor = Executors.newSingleThreadScheduledExecutor()) {
            var backoff = new WorkerBackoff(provider(container), executor);
            backoff.pause(false);
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> verify(container).stop());
            assertThat(backoff.isPaused()).isTrue();
            verify(container, never()).start();
        }
    }

    @Test
    void contextShutdownDoesNotRestartWorkersOrThrowWhenExecutorCloses() {
        var container = mock(SimpleMessageListenerContainer.class);
        try (var executor = Executors.newSingleThreadScheduledExecutor(); var context = new StaticApplicationContext()) {
            var backoff = new WorkerBackoff(provider(container), executor);
            backoff.closing(new ContextClosedEvent(context));
            backoff.pause(true);
            assertThat(backoff.isPaused()).isTrue();
            verifyNoInteractions(container);
            executor.shutdownNow();
            var closedExecutor = new WorkerBackoff(provider(container), executor);
            assertThatCode(() -> closedExecutor.pause(true)).doesNotThrowAnyException();
            verifyNoInteractions(container);
        }
    }

    @SuppressWarnings("unchecked")
    private ObjectProvider<SimpleMessageListenerContainer> provider(SimpleMessageListenerContainer container) {
        ObjectProvider<SimpleMessageListenerContainer> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(container);
        return provider;
    }
}