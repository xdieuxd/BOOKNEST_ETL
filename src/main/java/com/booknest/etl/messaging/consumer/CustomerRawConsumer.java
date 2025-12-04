package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.messaging.producer.CustomerMessageProducer;
import com.booknest.etl.service.dq.DataQualityService;

import lombok.RequiredArgsConstructor;

/**
 * Consumer for Customer RAW queue.
 * Validates customers and routes to quality or error queue.
 */
@Component
@RequiredArgsConstructor
public class CustomerRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(CustomerRawConsumer.class);

    private final DataQualityService dataQualityService;
    private final CustomerMessageProducer customerProducer;

    @RabbitListener(queues = "${etl.queues.customer-raw}")
    public void handleCustomerRaw(UserRawMessage message) {
        try {
            log.debug("📥 Received customer {} from raw queue", message.getUserId());

            List<DqErrorDto> errors = dataQualityService.validateUser(message);

            if (errors.isEmpty()) {
                customerProducer.sendToQuality(message);
                log.info("✅ Customer {} validated → forwarded to quality queue", message.getUserId());
            } else {
                customerProducer.sendToError(message, errors.toString());
                log.warn("⚠️ Customer {} validation failed: {} → sent to error queue", 
                    message.getUserId(), errors);
            }

        } catch (Exception e) {
            log.error("❌ Error processing customer {} in raw queue: {}", 
                message.getUserId(), e.getMessage(), e);
            customerProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
