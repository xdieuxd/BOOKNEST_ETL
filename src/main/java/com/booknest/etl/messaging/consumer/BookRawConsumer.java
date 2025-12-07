package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.messaging.producer.BookMessageProducer;
import com.booknest.etl.repository.staging.StagingBookRepository;
import com.booknest.etl.service.dq.DataQualityService;
import com.booknest.etl.service.dq.DataNormalizationService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class BookRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookRawConsumer.class);

    private final DataNormalizationService dataNormalizationService;
    private final DataQualityService dataQualityService;
    private final BookMessageProducer bookProducer;
    private final StagingBookRepository stagingBookRepository;

    @RabbitListener(queues = "${etl.queues.book-raw}")
    public void handleBookRaw(BookRawMessage message) {
        try {
            log.info("RAW CONSUMER: Received book {} - PERSISTING TO STAGING_DB", message.getBookId());

            message = dataNormalizationService.normalize(message);

            try {
                stagingBookRepository.upsert(message, DataQualityStatus.RAW, null);
                log.info("Book {} inserted to staging with RAW status", message.getBookId());
            } catch (Exception insertError) {
                String errorMsg = "Cannot insert to staging DB: " + insertError.getMessage();
                log.error("Book {} - {}", message.getBookId(), errorMsg);
                bookProducer.sendToError(message, errorMsg);
                return; 
            }

            List<DqErrorDto> errors = dataQualityService.validateBook(message);

            if (errors.isEmpty()) {
                stagingBookRepository.upsert(message, DataQualityStatus.VALIDATED, null);
                bookProducer.sendToQuality(message);
                log.info("Book {} validated → forwarded to quality queue", message.getBookId());
            } else {
                stagingBookRepository.upsert(message, DataQualityStatus.REJECTED, errors.toString());
                bookProducer.sendToError(message, errors.toString());
                log.warn("Book {} validation failed: {} → sent to error queue", 
                    message.getBookId(), errors);
            }

        } catch (Exception e) {
            log.error("Unexpected error processing book {}: {}", 
                message.getBookId(), e.getMessage(), e);
            bookProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
