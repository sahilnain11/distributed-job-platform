package com.sahil.jobplatform.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class JobMessageListenerTests {

    private JobWorker worker;
    private WorkerBackoff backoff;
    private Channel channel;
    private JobMessageListener listener;

    @BeforeEach
    void setUp() {
        worker = mock(JobWorker.class);
        backoff = mock(WorkerBackoff.class);
        channel = mock(Channel.class);
        listener = new JobMessageListener(new ObjectMapper(), worker,
                new MessagingSettings(true, true, "jobs.exchange", "jobs.queue", "jobs.execute", 100, 5000, 10, 30),
                backoff, new SimpleMeterRegistry());
    }

    @Test
    void ackIsSentOnlyAfterTheWorkerReturnsItsCommittedOutcome() throws Exception {
        UUID id = UUID.randomUUID();
        when(worker.process(id, 30)).thenReturn(JobWorker.Outcome.ACK);
        listener.onMessage(message("{\"jobId\":\"" + id + "\"}"), channel);
        var order = inOrder(worker, channel);
        order.verify(worker).process(id, 30);
        order.verify(channel).basicAck(7, false);
        verify(channel, never()).basicReject(anyLong(), anyBoolean());
    }

    @Test
    void databaseFailureNeverAcknowledgesAndPausesIntakeWithBackoff() throws Exception {
        when(worker.process(any(), eq(30))).thenThrow(new IllegalStateException("unknown commit"));
        listener.onMessage(message("{\"jobId\":\"" + UUID.randomUUID() + "\"}"), channel);
        verifyNoInteractions(channel);
        verify(backoff).pause(true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"broken-json", "{}", "null", "{\"jobId\":42}", "{\"jobId\":\"not-uuid\"}", "{\"jobId\":\"1-1-1-1-1\"}"})
    void malformedNotificationIsRejectedWithoutRunningOrRequeue(String body) throws Exception {
        listener.onMessage(message(body), channel);
        verify(channel).basicReject(7, false);
        verifyNoInteractions(worker);
    }

    @Test
    void confirmedMissingJobIsRejectedNotMarkedCompleted() throws Exception {
        when(worker.process(any(), eq(30))).thenReturn(JobWorker.Outcome.REJECT);
        listener.onMessage(message("{\"jobId\":\"" + UUID.randomUUID() + "\"}"), channel);
        verify(channel).basicReject(7, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void unresolvedWorkIsNotAckedOrFalselyFailed() throws Exception {
        when(worker.process(any(), eq(30))).thenReturn(JobWorker.Outcome.PAUSE);
        listener.onMessage(message("{\"jobId\":\"" + UUID.randomUUID() + "\"}"), channel);
        verifyNoInteractions(channel);
        verify(backoff).pause(true);
    }

    @Test
    void alreadyPausedIntakeDoesNotRunMoreWork() throws Exception {
        when(backoff.isPaused()).thenReturn(true);
        listener.onMessage(message("{}"), channel);
        verifyNoInteractions(worker, channel);
    }

    private Message message(String body) {
        var properties = new MessageProperties();
        properties.setDeliveryTag(7);
        return new Message(body.getBytes(StandardCharsets.UTF_8), properties);
    }
}