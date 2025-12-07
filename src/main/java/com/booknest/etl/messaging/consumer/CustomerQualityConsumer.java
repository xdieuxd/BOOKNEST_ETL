package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.service.load.SourceDbLoaderService;
import com.booknest.etl.service.transform.TransformService;
import com.booknest.etl.staging.StagingCustomerRepository;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class CustomerQualityConsumer {

    private static final Logger log = LoggerFactory.getLogger(CustomerQualityConsumer.class);

    private final TransformService transformService;
    private final StagingCustomerRepository stagingCustomerRepository;
    private final SourceDbLoaderService sourceDbLoaderService;

    @RabbitListener(queues = "${etl.queues.customer-quality}")
    @Transactional
    public void handleCustomerQuality(UserRawMessage message) {
        try {
            log.debug("Received customer {} from quality queue", message.getUserId());

            UserRawMessage transformed = transformService.transformUser(message);
            stagingCustomerRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            log.debug("Saved customer {} to staging_db", transformed.getUserId());

            sourceDbLoaderService.loadCustomersToSource();

            log.info("Customer {} processed: quality queue → transform → staging → source_db", 
                transformed.getUserId());

        } catch (Exception e) {
            log.error("Error processing customer {} in quality queue: {}", 
                message.getUserId(), e.getMessage(), e);
        }
    }
}
