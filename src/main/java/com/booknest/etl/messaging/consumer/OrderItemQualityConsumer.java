package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.repository.staging.StagingOrderItemRepository;
import com.booknest.etl.service.load.SourceDbLoaderService;
import com.booknest.etl.service.transform.TransformService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class OrderItemQualityConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderItemQualityConsumer.class);

    private final TransformService transformService;
    private final StagingOrderItemRepository stagingOrderItemRepository;
    private final SourceDbLoaderService sourceDbLoaderService;

    @RabbitListener(queues = "${etl.queues.orderitem-quality}")
    @Transactional
    public void handleOrderItemQuality(OrderItemRawMessage message) {
        try {
            log.debug("Received order item (book={}) from quality queue", message.getBookId());

            OrderItemRawMessage transformed = transformService.transformOrderItemPublic(message);

            log.debug("Saved order item to staging_db");

            sourceDbLoaderService.loadOrderItemsToSource();
            log.info("Order item processed: quality queue → transform → staging_db → source_db");

        } catch (Exception e) {
            log.error("Error processing order item in quality queue: {}", e.getMessage(), e);
        }
    }
}
