package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.repository.staging.StagingBookRepository;
import com.booknest.etl.service.load.SourceDbLoaderService;
import com.booknest.etl.service.transform.TransformService;

import lombok.RequiredArgsConstructor;

/**
 * Consumer for Book QUALITY queue.
 * Transforms validated books and loads to staging_db → source_db.
 * 
 * Flow: etl.book.quality → transform → staging_db → source_db
 */
@Component
@RequiredArgsConstructor
public class BookQualityConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookQualityConsumer.class);

    private final TransformService transformService;
    private final StagingBookRepository stagingBookRepository;
    private final SourceDbLoaderService sourceDbLoaderService;

    @RabbitListener(queues = "${etl.queues.book-quality}")
    @Transactional
    public void handleBookQuality(BookRawMessage message) {
        try {
            log.debug("📥 Received book {} from quality queue", message.getBookId());

            // Step 1: Transform (normalize, capitalize, etc.)
            BookRawMessage transformed = transformService.transformBook(message);

            // Step 2: Save to staging_db
            stagingBookRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            log.debug("💾 Saved book {} to staging_db", transformed.getBookId());

            // Step 3: Load to source_db (books + authors + categories + junctions)
            sourceDbLoaderService.loadBooksToSource();

            log.info("✅ Book {} processed: quality queue → transform → staging → source_db", 
                transformed.getBookId());

        } catch (Exception e) {
            log.error("❌ Error processing book {} in quality queue: {}", 
                message.getBookId(), e.getMessage(), e);
            // TODO: Send to DLQ (Dead Letter Queue) hoặc retry logic
        }
    }
}
