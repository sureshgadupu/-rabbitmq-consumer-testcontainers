package com.fullstackcode.sb.rabbitmq.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fullstackcode.sb.rabbitmq.consumer.listener.RabbitMQConsummer;
import com.fullstackcode.sb.rabbitmq.consumer.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@Import(dev.fullstackcode.sb.rabbitmq.consumer.config.RabbitMQTestConfiguration.class)
@ExtendWith(OutputCaptureExtension.class)
@DirtiesContext
@Slf4j
public class RabbitMQConsumerIT {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private  RabbitAdmin rabbitAdmin;

    @Autowired
    private TestRestTemplate testRestTemplate;

    static RabbitMQContainer rabbitMQContainer ;


    static {
        rabbitMQContainer = new RabbitMQContainer("rabbitmq:3.10.6-management-alpine")
                .withStartupTimeout(Duration.of(100, SECONDS));

        rabbitMQContainer.start();

    }

    @TestConfiguration
    public static class RabbitMQTestConfiguration {

        @Bean
        Queue queueA() {
            return new Queue("queue.A", false);
        }

        @Bean
        Queue queueB() {
            return new Queue("queue.B", false);
        }


        @Bean
        ApplicationRunner runner(ConnectionFactory cf) {
            return args -> cf.createConnection().close();
        }

        @Bean
        MessageConverter messageConverter() {
            return new Jackson2JsonMessageConverter();
        }

        @Bean
        RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
            RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
            rabbitTemplate.setMessageConverter(messageConverter());
            return rabbitTemplate;
        }

    }


    @Test
    public void testReceiveMessageFromQueueA(CapturedOutput output) throws Exception {

        Event event = new Event();
        event.setId(1);
        event.setName("Event A");

        rabbitTemplate.convertSendAndReceive("queue.A",event);

        Assertions.assertThat(output).contains("Event(id=1, name=Event A)");

    }


    @Test
    public void testReceiveMessageFromQueueB(CapturedOutput output) throws Exception {

        Event event = new Event();
        event.setId(1);
        event.setName("Event B");

        rabbitTemplate.convertSendAndReceive("queue.B",event);

        Assertions.assertThat(output).contains("Event(id=1, name=Event B)");

    }



    @DynamicPropertySource
    public static void properties(DynamicPropertyRegistry registry) {
        log.info("url ->{}",rabbitMQContainer.getAmqpUrl());
        log.info("port ->{}",rabbitMQContainer.getHttpPort());


        registry.add("spring.rabbitmq.host",() -> rabbitMQContainer.getHost());
        registry.add("spring.rabbitmq.port",() -> rabbitMQContainer.getAmqpPort());

    }

}
