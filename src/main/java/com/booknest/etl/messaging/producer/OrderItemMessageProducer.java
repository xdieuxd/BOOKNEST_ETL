package com.booknest.etl.messaging.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.OrderItemRawMessage;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderItemMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderItemMessageProducer.class);
    private final RabbitTemplate rabbitTemplate;

    @Value("${etl.exchange}")
    private String exchange;

    public void sendToRaw(OrderItemRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "orderitem.raw", message);
            log.debug("Sent order item (book={}) to raw queue", message.getBookId());
        } catch (Exception e) {
            log.error("Failed to send order item to raw queue: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send message to raw queue", e);
        }
    }

    public void sendToQuality(OrderItemRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "orderitem.quality", message);
            log.debug("Sent order item (book={}) to quality queue", message.getBookId());
        } catch (Exception e) {
            log.error("Failed to send order item to quality queue: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send message to quality queue", e);
        }
    }

    public void sendToError(OrderItemRawMessage message, String errorReason) {
        try {
            rabbitTemplate.convertAndSend(exchange, "orderitem.error", message);
            log.warn("Sent order item to error queue: {}", errorReason);
        } catch (Exception e) {
            log.error("Failed to send order item to error queue: {}", e.getMessage(), e);
        }
    }
}
