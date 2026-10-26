package com.sahil.jobplatform.messaging;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ScheduledThreadPoolExecutor;

@Configuration
@EnableConfigurationProperties(ReliabilitySettings.class)
public class ReliabilityConfiguration {

    @Bean(destroyMethod = "shutdownNow")
    ScheduledThreadPoolExecutor attemptSupervisorExecutor() {
        var executor = new ScheduledThreadPoolExecutor(4, Thread.ofPlatform()
                .name("attempt-supervisor-", 0).daemon(true).factory());
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return executor;
    }
}