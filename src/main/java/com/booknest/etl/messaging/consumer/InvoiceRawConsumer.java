package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.messaging.producer.InvoiceMessageProducer;
import com.booknest.etl.repository.staging.StagingInvoiceRepository;
import com.booknest.etl.service.dq.DataQualityService;
import com.booknest.etl.service.dq.DataNormalizationService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class InvoiceRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(InvoiceRawConsumer.class);

    private final DataNormalizationService dataNormalizationService;
    private final DataQualityService dataQualityService;
    private final InvoiceMessageProducer invoiceProducer;
    private final StagingInvoiceRepository stagingInvoiceRepository;

    @RabbitListener(queues = "${etl.queues.invoice-raw}")
    public void handleInvoiceRaw(InvoiceRawMessage message) {
        try {
            log.info("RAW CONSUMER: Received invoice {} - PERSISTING TO STAGING_DB", message.getInvoiceId());

            message = dataNormalizationService.normalize(message);

            try {
                stagingInvoiceRepository.upsert(message, DataQualityStatus.RAW, null);
                log.info("Invoice {} inserted to staging with RAW status", message.getInvoiceId());
            } catch (Exception insertError) {
                String errorMsg = "Cannot insert to staging DB: " + insertError.getMessage();
                log.error("Invoice {} - {}", message.getInvoiceId(), errorMsg);
                invoiceProducer.sendToError(message, errorMsg);
                return;
            }
            
            List<DqErrorDto> errors = dataQualityService.validateInvoice(message);

            if (errors.isEmpty()) {
                stagingInvoiceRepository.upsert(message, DataQualityStatus.VALIDATED, null);
                invoiceProducer.sendToQuality(message);
                log.info("Invoice {} validated → forwarded to quality queue", message.getInvoiceId());
            } else {
                stagingInvoiceRepository.upsert(message, DataQualityStatus.REJECTED, errors.toString());
                invoiceProducer.sendToError(message, errors.toString());
                log.warn("Invoice {} validation failed: {}", message.getInvoiceId(), errors);
            }
        } catch (Exception e) {
            log.error("Unexpected error processing invoice {}: {}", message.getInvoiceId(), e.getMessage(), e);
            invoiceProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
