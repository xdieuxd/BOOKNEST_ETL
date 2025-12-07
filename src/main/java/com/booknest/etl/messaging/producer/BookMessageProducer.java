package com.booknest.etl.messaging.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.BookRawMessage;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class BookMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(BookMessageProducer.class);

    private final RabbitTemplate rabbitTemplate;

    @Value("${etl.exchange}")
    private String exchange;

    public void sendToRaw(BookRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "book.raw", message);
            log.debug("Sent book {} to raw queue", message.getBookId());
        } catch (Exception e) {
            log.error("Failed to send book {} to raw queue: {}", message.getBookId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to raw queue", e);
        }
    }

    public void sendToQuality(BookRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "book.quality", message);
            log.debug("Sent book {} to quality queue", message.getBookId());
        } catch (Exception e) {
            log.error("Failed to send book {} to quality queue: {}", message.getBookId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to quality queue", e);
        }
    }

    public void sendToError(BookRawMessage message, String errorReason) {
        try {
            rabbitTemplate.convertAndSend(exchange, "book.error", message);
            log.warn("Sent book {} to error queue: {}", message.getBookId(), errorReason);
        } catch (Exception e) {
            log.error("Failed to send book {} to error queue: {}", message.getBookId(), e.getMessage(), e);
        }
    }
}
