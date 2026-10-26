package com.sahil.jobplatform.messaging;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(MessagingSettings.class)
@ConditionalOnProperty(name = "platform.messaging.enabled", havingValue = "true")
public class MessagingConfiguration {

    @Bean
    DirectExchange jobExchange(MessagingSettings settings) {
        return new DirectExchange(settings.exchange(), true, false);
    }

    @Bean
    Queue jobQueue(MessagingSettings settings) {
        return new Queue(settings.queue(), true, false, false);
    }

    @Bean
    Binding jobBinding(Queue jobQueue, DirectExchange jobExchange, MessagingSettings settings) {
        return BindingBuilder.bind(jobQueue).to(jobExchange).with(settings.routingKey());
    }

    @Bean
    RabbitTemplate jobRabbitTemplate(ConnectionFactory connections) {
        RabbitTemplate template = new RabbitTemplate(connections);
        template.setMandatory(true);
        return template;
    }

    @Bean(destroyMethod = "shutdownNow")
    ScheduledExecutorService workerBackoffExecutor() {
        return Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .name("worker-infrastructure-backoff").daemon(true).factory());
    }

    @Bean
    ThreadPoolTaskScheduler taskScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(3);
        scheduler.setThreadNamePrefix("platform-poller-");
        return scheduler;
    }

    @Bean
    SimpleMessageListenerContainer jobWorkerContainer(ConnectionFactory connections,
                                                      MessagingSettings settings, JobMessageListener listener) {
        var container = new SimpleMessageListenerContainer(connections);
        container.setQueueNames(settings.queue());
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        // Ten consumers * one unacknowledged delivery = ten worker-wide slots.
        // Execution is on the consumer thread, never an unbounded local queue.
        container.setConcurrentConsumers(settings.workerConcurrency());
        container.setMaxConcurrentConsumers(settings.workerConcurrency());
        container.setPrefetchCount(1);
        container.setRecoveryInterval(5000);
        container.setShutdownTimeout(10000);
        container.setAutoStartup(settings.workerEnabled());
        container.setMessageListener(listener);
        return container;
    }
}