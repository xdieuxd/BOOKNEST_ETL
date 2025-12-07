package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.repository.staging.StagingInvoiceRepository;
import com.booknest.etl.service.load.SourceDbLoaderService;
import com.booknest.etl.service.transform.TransformService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class InvoiceQualityConsumer {

    private static final Logger log = LoggerFactory.getLogger(InvoiceQualityConsumer.class);

    private final TransformService transformService;
    private final StagingInvoiceRepository stagingInvoiceRepository;
    private final SourceDbLoaderService sourceDbLoaderService;

    @RabbitListener(queues = "${etl.queues.invoice-quality}")
    @Transactional
    public void handleInvoiceQuality(InvoiceRawMessage message) {
        try {
            log.debug("Received invoice {} from quality queue", message.getInvoiceId());

            InvoiceRawMessage transformed = transformService.transformInvoice(message);
            stagingInvoiceRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            log.debug("Saved invoice {} to staging_db", transformed.getInvoiceId());

            sourceDbLoaderService.loadInvoicesToSource();
            log.info("Invoice {} processed: quality queue → transform → staging_db → source_db (partial)", 
                transformed.getInvoiceId());

        } catch (Exception e) {
            log.error("Error processing invoice {} in quality queue: {}", 
                message.getInvoiceId(), e.getMessage(), e);
        }
    }
}
