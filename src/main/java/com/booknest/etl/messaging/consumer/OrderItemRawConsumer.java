package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.messaging.producer.OrderItemMessageProducer;
import com.booknest.etl.repository.staging.StagingOrderItemRepository;
import com.booknest.etl.service.dq.DataQualityService;
import com.booknest.etl.service.dq.DataNormalizationService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderItemRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderItemRawConsumer.class);

    private final DataNormalizationService dataNormalizationService;
    private final DataQualityService dataQualityService;
    private final OrderItemMessageProducer orderItemProducer;
    private final StagingOrderItemRepository stagingOrderItemRepository;

    @RabbitListener(queues = "${etl.queues.orderitem-raw}")
    public void handleOrderItemRaw(OrderItemRawMessage message) {
        try {
            log.info("RAW CONSUMER: Received order item (book={}) - PERSISTING TO STAGING_DB", message.getBookId());

            message = dataNormalizationService.normalize(message);
            
            try {
                stagingOrderItemRepository.upsert(message, DataQualityStatus.RAW, null);
                log.info("Order item (book={}) inserted to staging with RAW status", message.getBookId());
            } catch (Exception insertError) {
                String errorMsg = "Cannot insert to staging DB: " + insertError.getMessage();
                log.error("Order item (book={}) - {}", message.getBookId(), errorMsg);
                orderItemProducer.sendToError(message, errorMsg);
                return;
            }
            
            List<DqErrorDto> errors = dataQualityService.validateOrderItem(message);

            if (errors.isEmpty()) {
                stagingOrderItemRepository.upsert(message, DataQualityStatus.VALIDATED, null);
                orderItemProducer.sendToQuality(message);
                log.info("Order item (book={}) validated → forwarded to quality queue", message.getBookId());
            } else {
                stagingOrderItemRepository.upsert(message, DataQualityStatus.REJECTED, errors.toString());
                orderItemProducer.sendToError(message, errors.toString());
                log.warn("Order item (book={}) validation failed: {}", message.getBookId(), errors);
            }
        } catch (Exception e) {
            log.error("Error processing order item: {}", e.getMessage(), e);
            orderItemProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
