package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.messaging.producer.OrderMessageProducer;
import com.booknest.etl.repository.staging.StagingOrderRepository;
import com.booknest.etl.service.dq.DataQualityService;
import com.booknest.etl.service.dq.DataNormalizationService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderRawConsumer.class);

    private final DataNormalizationService dataNormalizationService;
    private final DataQualityService dataQualityService;
    private final OrderMessageProducer orderProducer;
    private final StagingOrderRepository stagingOrderRepository;

    @RabbitListener(queues = "${etl.queues.order-raw}")
    public void handleOrderRaw(OrderRawMessage message) {
        try {
            log.info("RAW CONSUMER: Received order {} - PERSISTING TO STAGING_DB", message.getOrderId());

            message = dataNormalizationService.normalize(message);

            try {
                stagingOrderRepository.upsert(message, DataQualityStatus.RAW, null);
                log.info("Order {} inserted to staging with RAW status", message.getOrderId());
            } catch (Exception insertError) {
                String errorMsg = "Cannot insert to staging DB: " + insertError.getMessage();
                log.error("Order {} - {}", message.getOrderId(), errorMsg);
                orderProducer.sendToError(message, errorMsg);
                return;
            }
            
            List<DqErrorDto> errors = dataQualityService.validateOrder(message);

            if (errors.isEmpty()) {
                stagingOrderRepository.upsert(message, DataQualityStatus.VALIDATED, null);
                orderProducer.sendToQuality(message);
                log.info("Order {} validated → forwarded to quality queue", message.getOrderId());
            } else {
                stagingOrderRepository.upsert(message, DataQualityStatus.REJECTED, errors.toString());
                orderProducer.sendToError(message, errors.toString());
                log.warn("Order {} validation failed: {}", message.getOrderId(), errors);
            }
        } catch (Exception e) {
            log.error("Unexpected error processing order {}: {}", message.getOrderId(), e.getMessage(), e);
            orderProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
