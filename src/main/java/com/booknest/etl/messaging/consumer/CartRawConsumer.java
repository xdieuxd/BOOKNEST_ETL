package com.booknest.etl.messaging.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.messaging.producer.CartMessageProducer;
import com.booknest.etl.repository.staging.StagingCartRepository;
import com.booknest.etl.service.dq.DataQualityService;
import com.booknest.etl.service.dq.DataNormalizationService;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class CartRawConsumer {

    private static final Logger log = LoggerFactory.getLogger(CartRawConsumer.class);

    private final DataNormalizationService dataNormalizationService;
    private final DataQualityService dataQualityService;
    private final CartMessageProducer cartProducer;
    private final StagingCartRepository stagingCartRepository;

    @RabbitListener(queues = "${etl.queues.cart-raw}")
    public void handleCartRaw(CartRawMessage message) {
        try {
            log.info("RAW CONSUMER: Received cart {} - PERSISTING TO STAGING_DB", message.getCartId());

            message = dataNormalizationService.normalize(message);

            try {
                stagingCartRepository.upsert(message, DataQualityStatus.RAW, null);
                log.info("Cart {} inserted to staging with RAW status", message.getCartId());
            } catch (Exception insertError) {
                String errorMsg = "Cannot insert to staging DB: " + insertError.getMessage();
                log.error("Cart {} - {}", message.getCartId(), errorMsg);
                cartProducer.sendToError(message, errorMsg);
                return;
            }
            
            List<DqErrorDto> errors = dataQualityService.validateCart(message);

            if (errors.isEmpty()) {
                stagingCartRepository.upsert(message, DataQualityStatus.VALIDATED, null);
                cartProducer.sendToQuality(message);
                log.info("Cart {} validated → forwarded to quality queue", message.getCartId());
            } else {
                stagingCartRepository.upsert(message, DataQualityStatus.REJECTED, errors.toString());
                cartProducer.sendToError(message, errors.toString());
                log.warn("Cart {} validation failed: {}", message.getCartId(), errors);
            }
        } catch (Exception e) {
            log.error("Unexpected error processing cart {}: {}", message.getCartId(), e.getMessage(), e);
            cartProducer.sendToError(message, "Processing error: " + e.getMessage());
        }
    }
}
