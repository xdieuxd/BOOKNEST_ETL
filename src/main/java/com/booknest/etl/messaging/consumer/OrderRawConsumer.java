package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.messaging.producer.OrderMessageProducer;
import com.booknest.etl.service.dq.DataQualityService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderRawConsumer.class);
    private final DataQualityService dataQualityService;
    private final OrderMessageProducer orderProducer;

    @RabbitListener(queues = "${etl.queues.order-raw}")
    public void handleOrderRaw(OrderRawMessage message) {
        try {
            log.debug("📥 Received order {} from raw queue", message.getOrderId());
            List<DqErrorDto> errors = dataQualityService.validateOrder(message);

            if (errors.isEmpty()) {
                orderProducer.sendToQuality(message);
                log.info("✅ Order {} validated → forwarded to quality queue", message.getOrderId());
            } else {
                orderProducer.sendToError(message, errors.toString());
                log.warn("⚠️ Order {} validation failed: {}", message.getOrderId(), errors);
            }
        } catch (Exception e) {
            log.error("❌ Error processing order {}: {}", message.getOrderId(), e.getMessage(), e);
            orderProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
