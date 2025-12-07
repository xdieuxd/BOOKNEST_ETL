package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.repository.staging.StagingOrderRepository;
import com.booknest.etl.service.load.SourceDbLoaderService;
import com.booknest.etl.service.transform.TransformService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderQualityConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderQualityConsumer.class);

    private final TransformService transformService;
    private final StagingOrderRepository stagingOrderRepository;
    private final SourceDbLoaderService sourceDbLoaderService;

    @RabbitListener(queues = "${etl.queues.order-quality}")
    @Transactional
    public void handleOrderQuality(OrderRawMessage message) {
        try {
            log.debug("Received order {} from quality queue", message.getOrderId());

            OrderRawMessage transformed = transformService.transformOrder(message);
            stagingOrderRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            log.debug("Saved order {} to staging_db", transformed.getOrderId());

            sourceDbLoaderService.loadOrdersToSource();

            log.info("Order {} processed: quality queue → transform → staging → source_db", 
                transformed.getOrderId());

        } catch (Exception e) {
            log.error("Error processing order {} in quality queue: {}", 
                message.getOrderId(), e.getMessage(), e);
        }
    }
}
