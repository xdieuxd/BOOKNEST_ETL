package com.booknest.etl.messaging.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.UserRawMessage;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class CustomerMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(CustomerMessageProducer.class);

    private final RabbitTemplate rabbitTemplate;

    @Value("${etl.exchange}")
    private String exchange;

    public void sendToRaw(UserRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "customer.raw", message);
            log.debug("Sent customer {} to raw queue", message.getUserId());
        } catch (Exception e) {
            log.error("Failed to send customer {} to raw queue: {}", message.getUserId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to raw queue", e);
        }
    }

    public void sendToQuality(UserRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "customer.quality", message);
            log.debug("Sent customer {} to quality queue", message.getUserId());
        } catch (Exception e) {
            log.error("Failed to send customer {} to quality queue: {}", message.getUserId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to quality queue", e);
        }
    }

    public void sendToError(UserRawMessage message, String errorReason) {
        try {
            rabbitTemplate.convertAndSend(exchange, "customer.error", message);
            log.warn("Sent customer {} to error queue: {}", message.getUserId(), errorReason);
        } catch (Exception e) {
            log.error("Failed to send customer {} to error queue: {}", message.getUserId(), e.getMessage(), e);
        }
    }
}
