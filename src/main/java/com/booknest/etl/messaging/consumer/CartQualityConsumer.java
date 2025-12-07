package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.repository.staging.StagingCartRepository;
import com.booknest.etl.service.load.SourceDbLoaderService;
import com.booknest.etl.service.transform.TransformService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class CartQualityConsumer {

    private static final Logger log = LoggerFactory.getLogger(CartQualityConsumer.class);

    private final TransformService transformService;
    private final StagingCartRepository stagingCartRepository;
    private final SourceDbLoaderService sourceDbLoaderService;

    @RabbitListener(queues = "${etl.queues.cart-quality}")
    @Transactional
    public void handleCartQuality(CartRawMessage message) {
        try {
            log.debug("Received cart {} from quality queue", message.getCartId());

            CartRawMessage transformed = transformService.transformCart(message);
            stagingCartRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            log.debug("Saved cart {} to staging_db", transformed.getCartId());

            sourceDbLoaderService.loadCartsToSource();
            log.info("Cart {} processed: quality queue → transform → staging_db → source_db", 
                transformed.getCartId());

        } catch (Exception e) {
            log.error("Error processing cart {} in quality queue: {}", 
                message.getCartId(), e.getMessage(), e);
        }
    }
}
