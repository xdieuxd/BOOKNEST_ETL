package com.booknest.etl.messaging.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class EtlMessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(EtlMessagePublisher.class);

    private final RabbitTemplate rabbitTemplate;

    @Value("${etl.exchange}")
    private String etlExchange;

    public EtlMessagePublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendRaw(Object payload) {
        send("raw", payload);
    }

    public void sendQuality(Object payload) {
        send("quality", payload);
    }

    public void sendError(Object payload) {
        send("error", payload);
    }

    private void send(String routingKey, Object payload) {
        rabbitTemplate.convertAndSend(etlExchange, routingKey, payload);
        log.debug("Sent payload to routing key {}: {}", routingKey, payload);
    }
}
