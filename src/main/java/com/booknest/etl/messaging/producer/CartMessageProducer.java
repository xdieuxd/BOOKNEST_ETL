package com.booknest.etl.messaging.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.CartRawMessage;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class CartMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(CartMessageProducer.class);
    private final RabbitTemplate rabbitTemplate;

    @Value("${etl.exchange}")
    private String exchange;

    public void sendToRaw(CartRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "cart.raw", message);
            log.debug("📤 Sent cart {} to raw queue", message.getCartId());
        } catch (Exception e) {
            log.error("❌ Failed to send cart {} to raw queue: {}", message.getCartId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to raw queue", e);
        }
    }

    public void sendToQuality(CartRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "cart.quality", message);
            log.debug("📤 Sent cart {} to quality queue", message.getCartId());
        } catch (Exception e) {
            log.error("❌ Failed to send cart {} to quality queue: {}", message.getCartId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to quality queue", e);
        }
    }

    public void sendToError(CartRawMessage message, String errorReason) {
        try {
            rabbitTemplate.convertAndSend(exchange, "cart.error", message);
            log.warn("⚠️ Sent cart {} to error queue: {}", message.getCartId(), errorReason);
        } catch (Exception e) {
            log.error("❌ Failed to send cart {} to error queue: {}", message.getCartId(), e.getMessage(), e);
        }
    }
}
