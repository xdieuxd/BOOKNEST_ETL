package com.booknest.etl.messaging.consumer;

import java.time.OffsetDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.messaging.producer.EtlMessagePublisher;

import lombok.RequiredArgsConstructor;

/**
 * Receives validated messages from etl.raw queue and forwards to etl.quality queue.
 * Validation already done in EtlUploadController before publishing to raw queue.
 * This consumer just forwards messages to quality queue for transformation + loading.
 */
@Component
@RabbitListener(queues = "${etl.queues.raw}")
@RequiredArgsConstructor
public class RawMessageListener {

    private static final Logger log = LoggerFactory.getLogger(RawMessageListener.class);

    private final EtlMessagePublisher publisher;

    @RabbitHandler
    public void handleBook(BookRawMessage message) {
        log.debug("📥 Received book {} from raw queue → forwarding to quality queue", message.getBookId());
        publisher.sendQuality(message);
    }

    @RabbitHandler
    public void handleUser(UserRawMessage message) {
        log.debug("📥 Received customer {} from raw queue → forwarding to quality queue", message.getUserId());
        publisher.sendQuality(message);
    }

    @RabbitHandler
    public void handleOrder(OrderRawMessage message) {
        log.debug("📥 Received order {} from raw queue → forwarding to quality queue", message.getOrderId());
        publisher.sendQuality(message);
    }

    @RabbitHandler
    public void handleCart(CartRawMessage message) {
        log.debug("📥 Received cart {} from raw queue → forwarding to quality queue", message.getCartId());
        publisher.sendQuality(message);
    }

    @RabbitHandler
    public void handleInvoice(InvoiceRawMessage message) {
        log.debug("📥 Received invoice {} from raw queue → forwarding to quality queue", message.getInvoiceId());
        publisher.sendQuality(message);
    }

    @RabbitHandler
    public void handleOrderItem(OrderItemRawMessage message) {
        log.debug("📥 Received order item (book={}) from raw queue → forwarding to quality queue", message.getBookId());
        publisher.sendQuality(message);
    }
}
