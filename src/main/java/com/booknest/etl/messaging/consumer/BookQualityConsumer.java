package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.messaging.producer.BookMessageProducer;
import com.booknest.etl.repository.staging.StagingBookRepository;
import com.booknest.etl.service.load.SourceDbLoaderService;
import com.booknest.etl.service.transform.TransformService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class BookQualityConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookQualityConsumer.class);

    private final TransformService transformService;
    private final StagingBookRepository stagingBookRepository;
    private final SourceDbLoaderService sourceDbLoaderService;
    private final BookMessageProducer bookMessageProducer;

    @RabbitListener(queues = "${etl.queues.book-quality}")
    @Transactional
    public void handleBookQuality(BookRawMessage message) {
        try {
            log.debug("Received book {} from quality queue", message.getBookId());

            BookRawMessage transformed = transformService.transformBook(message);

            stagingBookRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            log.debug("Saved book {} to staging_db", transformed.getBookId());

            sourceDbLoaderService.loadBooksToSource();

            log.info("Book {} processed: quality -> transform -> staging -> source_db", transformed.getBookId());

        } catch (Exception e) {
            log.error("Error processing book {} in quality queue: {}", message.getBookId(), e.getMessage(), e);
            stagingBookRepository.upsert(message, DataQualityStatus.REJECTED, e.getMessage());
            bookMessageProducer.sendToError(message, e.getMessage());
        }
    }
}
