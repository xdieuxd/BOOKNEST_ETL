package com.booknest.etl.messaging.consumer;

import java.time.OffsetDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dto.EtlResultDto;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.messaging.producer.EtlMessagePublisher;
import com.booknest.etl.repository.staging.DqResultRepository;
import com.booknest.etl.service.transform.TransformService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.booknest.etl.service.dq.DataQualityService;

import lombok.RequiredArgsConstructor;

@Component
@RabbitListener(queues = "${etl.queues.raw}")
@RequiredArgsConstructor
public class RawMessageListener {

    private static final Logger log = LoggerFactory.getLogger(RawMessageListener.class);

    private final DataQualityService dataQualityService;
    private final EtlMessagePublisher publisher;
    private final DqResultRepository dqResultRepository;
    private final ObjectMapper objectMapper;
    private final TransformService transformService;

    @RabbitHandler
    public void handleBook(BookRawMessage message) {
        processMessage("BOOK", message.getBookId(), dataQualityService.validateBook(message), message);
    }

    @RabbitHandler
    public void handleUser(UserRawMessage message) {
        processMessage("CUSTOMER", message.getUserId(), dataQualityService.validateUser(message), message);
    }

    @RabbitHandler
    public void handleOrder(OrderRawMessage message) {
        processMessage("ORDER", message.getOrderId(), dataQualityService.validateOrder(message), message);
    }

    @RabbitHandler
    public void handleCart(CartRawMessage message) {
        processMessage("CART", message.getCartId(), dataQualityService.validateCart(message), message);
    }

    @RabbitHandler
    public void handleInvoice(InvoiceRawMessage message) {
        processMessage("INVOICE", message.getInvoiceId(), dataQualityService.validateInvoice(message), message);
    }

    private void processMessage(String entityType,
                                String entityKey,
                                List<DqErrorDto> errors,
                                Object payload) {
        if (errors.isEmpty()) {
            Object transformed = transformPayload(payload);
            publisher.sendQuality(transformed);
            log.debug("Sent {} {} to quality queue", entityType, entityKey);
            dqResultRepository.saveResult(entityType, entityKey, DataQualityStatus.PASSED, null);
        } else {
            EtlResultDto errorResult = EtlResultDto.builder()
                    .entityType(entityType)
                    .entityKey(entityKey)
                    .success(false)
                    .errors(errors)
                    .message("Failed data quality validation")
                    .processedAt(OffsetDateTime.now())
                    .build();
            publisher.sendError(errorResult);
            dqResultRepository.saveResult(entityType, entityKey, DataQualityStatus.FAILED, errorsToJson(errors));
            log.warn("Message {} {} failed DQ with {} errors", entityType, entityKey, errors.size());
        }
    }

    private String errorsToJson(List<DqErrorDto> errors) {
        try {
            return objectMapper.writeValueAsString(errors);
        } catch (JsonProcessingException e) {
            log.error("Cannot serialize errors to JSON", e);
            return null;
        }
    }

    private Object transformPayload(Object payload) {
        if (payload instanceof BookRawMessage book) {
            return transformService.transformBook(book);
        }
        if (payload instanceof UserRawMessage user) {
            return transformService.transformUser(user);
        }
        if (payload instanceof OrderRawMessage order) {
            return transformService.transformOrder(order);
        }
        if (payload instanceof CartRawMessage cart) {
            return transformService.transformCart(cart);
        }
        if (payload instanceof InvoiceRawMessage invoice) {
            return transformService.transformInvoice(invoice);
        }
        return payload;
    }
}
