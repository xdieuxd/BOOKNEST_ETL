package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.messaging.producer.CustomerMessageProducer;
import com.booknest.etl.staging.StagingCustomerRepository;
import com.booknest.etl.service.dq.DataQualityService;
import com.booknest.etl.service.dq.DataNormalizationService;

import lombok.RequiredArgsConstructor;


@Component
@RequiredArgsConstructor
public class CustomerRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(CustomerRawConsumer.class);

    private final DataNormalizationService dataNormalizationService;
    private final DataQualityService dataQualityService;
    private final CustomerMessageProducer customerProducer;
    private final StagingCustomerRepository stagingCustomerRepository;

    @RabbitListener(queues = "${etl.queues.customer-raw}")
    public void handleCustomerRaw(UserRawMessage message) {
        try {
            log.info("RAW CONSUMER: Received customer {} - PERSISTING TO STAGING_DB", message.getUserId());

            message = dataNormalizationService.normalize(message);

            try {
                stagingCustomerRepository.upsert(message, DataQualityStatus.RAW, null);
                log.info("Customer {} inserted to staging with RAW status", message.getUserId());
            } catch (Exception insertError) {
                String errorMsg = "Cannot insert to staging DB: " + insertError.getMessage();
                log.error("Customer {} - {}", message.getUserId(), errorMsg);
                customerProducer.sendToError(message, errorMsg);
                return;
            }

            List<DqErrorDto> errors = dataQualityService.validateUser(message);

            if (errors.isEmpty()) {
                stagingCustomerRepository.upsert(message, DataQualityStatus.VALIDATED, null);
                customerProducer.sendToQuality(message);
                log.info("Customer {} validated → forwarded to quality queue", message.getUserId());
            } else {
                stagingCustomerRepository.upsert(message, DataQualityStatus.REJECTED, errors.toString());
                customerProducer.sendToError(message, errors.toString());
                log.warn("Customer {} validation failed: {} → sent to error queue", 
                    message.getUserId(), errors);
            }

        } catch (Exception e) {
            log.error("Unexpected error processing customer {}: {}", 
                message.getUserId(), e.getMessage(), e);
            customerProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
