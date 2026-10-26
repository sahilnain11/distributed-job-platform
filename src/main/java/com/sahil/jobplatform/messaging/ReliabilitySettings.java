package com.sahil.jobplatform.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("platform.reliability")
public record ReliabilitySettings(
        @DefaultValue("10000") long heartbeatMs,
        @DefaultValue("1000") long controlIntervalMs,
        @DefaultValue("300000") long maxRuntimeMs,
        @DefaultValue("1000") long retryBaseMs,
        @DefaultValue("true") boolean pollersEnabled) {
    public ReliabilitySettings {
        if (heartbeatMs < 1 || controlIntervalMs < 1 || maxRuntimeMs < 1
                || retryBaseMs < 1 || retryBaseMs > 60000) {
            throw new IllegalArgumentException("Invalid reliability timing settings");
        }
    }
}