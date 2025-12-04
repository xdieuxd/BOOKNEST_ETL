package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.messaging.producer.BookMessageProducer;
import com.booknest.etl.service.dq.DataQualityService;

import lombok.RequiredArgsConstructor;

/**
 * Consumer for Book RAW queue.
 * Validates books and routes to quality or error queue.
 * 
 * Flow: etl.book.raw → validate → etl.book.quality (pass) or etl.book.errors (fail)
 */
@Component
@RequiredArgsConstructor
public class BookRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookRawConsumer.class);

    private final DataQualityService dataQualityService;
    private final BookMessageProducer bookProducer;

    @RabbitListener(queues = "${etl.queues.book-raw}")
    public void handleBookRaw(BookRawMessage message) {
        try {
            log.debug("📥 Received book {} from raw queue", message.getBookId());

            // Validate
            List<DqErrorDto> errors = dataQualityService.validateBook(message);

            if (errors.isEmpty()) {
                // Validation passed → send to quality queue
                bookProducer.sendToQuality(message);
                log.info("✅ Book {} validated → forwarded to quality queue", message.getBookId());
            } else {
                // Validation failed → send to error queue
                bookProducer.sendToError(message, errors.toString());
                log.warn("⚠️ Book {} validation failed: {} → sent to error queue", 
                    message.getBookId(), errors);
            }

        } catch (Exception e) {
            log.error("❌ Error processing book {} in raw queue: {}", 
                message.getBookId(), e.getMessage(), e);
            bookProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
