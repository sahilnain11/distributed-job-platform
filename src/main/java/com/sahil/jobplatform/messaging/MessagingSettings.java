package com.sahil.jobplatform.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("platform.messaging")
public record MessagingSettings(
        @DefaultValue("true") boolean publisherEnabled,
        @DefaultValue("true") boolean workerEnabled,
        @DefaultValue("jobs.exchange") String exchange,
        @DefaultValue("jobs.queue") String queue,
        @DefaultValue("jobs.execute") String routingKey,
        @DefaultValue("100") int batchSize,
        @DefaultValue("5000") long confirmTimeoutMs,
        @DefaultValue("10") int workerConcurrency,
        @DefaultValue("30") int leaseSeconds) {

    public MessagingSettings {
        if (batchSize < 1 || batchSize > 1000 || confirmTimeoutMs < 1
                || workerConcurrency < 1 || workerConcurrency > 100 || leaseSeconds < 1) {
            throw new IllegalArgumentException("Invalid messaging resource limits");
        }
    }
}