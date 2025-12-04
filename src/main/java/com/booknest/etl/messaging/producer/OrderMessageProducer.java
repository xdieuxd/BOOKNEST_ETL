package com.booknest.etl.messaging.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.OrderRawMessage;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderMessageProducer.class);
    private final RabbitTemplate rabbitTemplate;

    @Value("${etl.exchange}")
    private String exchange;

    public void sendToRaw(OrderRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "order.raw", message);
            log.debug("📤 Sent order {} to raw queue", message.getOrderId());
        } catch (Exception e) {
            log.error("❌ Failed to send order {} to raw queue: {}", message.getOrderId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to raw queue", e);
        }
    }

    public void sendToQuality(OrderRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "order.quality", message);
            log.debug("📤 Sent order {} to quality queue", message.getOrderId());
        } catch (Exception e) {
            log.error("❌ Failed to send order {} to quality queue: {}", message.getOrderId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to quality queue", e);
        }
    }

    public void sendToError(OrderRawMessage message, String errorReason) {
        try {
            rabbitTemplate.convertAndSend(exchange, "order.error", message);
            log.warn("⚠️ Sent order {} to error queue: {}", message.getOrderId(), errorReason);
        } catch (Exception e) {
            log.error("❌ Failed to send order {} to error queue: {}", message.getOrderId(), e.getMessage(), e);
        }
    }
}
