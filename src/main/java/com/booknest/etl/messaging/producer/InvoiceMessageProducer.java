package com.booknest.etl.messaging.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.InvoiceRawMessage;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class InvoiceMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(InvoiceMessageProducer.class);
    private final RabbitTemplate rabbitTemplate;

    @Value("${etl.exchange}")
    private String exchange;

    public void sendToRaw(InvoiceRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "invoice.raw", message);
            log.debug("Sent invoice {} to raw queue", message.getInvoiceId());
        } catch (Exception e) {
            log.error("Failed to send invoice {} to raw queue: {}", message.getInvoiceId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to raw queue", e);
        }
    }

    public void sendToQuality(InvoiceRawMessage message) {
        try {
            rabbitTemplate.convertAndSend(exchange, "invoice.quality", message);
            log.debug("Sent invoice {} to quality queue", message.getInvoiceId());
        } catch (Exception e) {
            log.error("Failed to send invoice {} to quality queue: {}", message.getInvoiceId(), e.getMessage(), e);
            throw new RuntimeException("Failed to send message to quality queue", e);
        }
    }

    public void sendToError(InvoiceRawMessage message, String errorReason) {
        try {
            rabbitTemplate.convertAndSend(exchange, "invoice.error", message);
            log.warn("Sent invoice {} to error queue: {}", message.getInvoiceId(), errorReason);
        } catch (Exception e) {
            log.error("Failed to send invoice {} to error queue: {}", message.getInvoiceId(), e.getMessage(), e);
        }
    }
}
