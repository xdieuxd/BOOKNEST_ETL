package com.booknest.etl.service.load;

import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.logging.EtlLog;
import com.booknest.etl.logging.EtlLogRepository;
import com.booknest.etl.repository.staging.StagingBookRepository;
import com.booknest.etl.repository.staging.StagingCartItemRepository;
import com.booknest.etl.repository.staging.StagingCartRepository;
import com.booknest.etl.repository.staging.StagingInvoiceRepository;
import com.booknest.etl.repository.staging.StagingOrderItemRepository;
import com.booknest.etl.repository.staging.StagingOrderRepository;
import com.booknest.etl.staging.StagingCustomerRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class StagingLoaderService {

    private static final Logger log = LoggerFactory.getLogger(StagingLoaderService.class);

    private final StagingBookRepository stagingBookRepository;
    private final StagingCustomerRepository stagingCustomerRepository;
    private final StagingOrderRepository stagingOrderRepository;
    private final StagingOrderItemRepository stagingOrderItemRepository;
    private final StagingCartRepository stagingCartRepository;
    private final StagingCartItemRepository stagingCartItemRepository;
    private final StagingInvoiceRepository stagingInvoiceRepository;
    private final EtlLogRepository etlLogRepository;

    @RabbitListener(queues = "${etl.queues.quality}")
    public void consumeQualityMessage(Object payload) {
        OffsetDateTime start = OffsetDateTime.now();
        try {
            if (payload instanceof BookRawMessage book) {
                stagingBookRepository.upsert(book, DataQualityStatus.VALIDATED, null);
                log.debug("Loaded book {} into staging", book.getBookId());
                saveLog("LOAD_BOOK", book.getBookId(), "SUCCESS", null, start);
            } else if (payload instanceof UserRawMessage user) {
                stagingCustomerRepository.upsert(user, DataQualityStatus.VALIDATED, null);
                log.debug("Loaded user {} into staging", user.getUserId());
                saveLog("LOAD_USER", user.getUserId(), "SUCCESS", null, start);
            } else if (payload instanceof OrderRawMessage order) {
                stagingOrderRepository.upsert(order, DataQualityStatus.VALIDATED, null);
                stagingOrderItemRepository.replaceItems(order.getOrderId(), order.getItems());
                log.debug("Loaded order {} into staging", order.getOrderId());
                saveLog("LOAD_ORDER", order.getOrderId(), "SUCCESS", null, start);
            } else if (payload instanceof CartRawMessage cart) {
                stagingCartRepository.upsert(cart, DataQualityStatus.VALIDATED, null);
                stagingCartItemRepository.replaceItems(cart.getCartId(), cart.getItems());
                log.debug("Loaded cart {} into staging", cart.getCartId());
                saveLog("LOAD_CART", cart.getCartId(), "SUCCESS", null, start);
            } else if (payload instanceof InvoiceRawMessage invoice) {
                stagingInvoiceRepository.upsert(invoice, DataQualityStatus.VALIDATED, null);
                log.debug("Loaded invoice {} into staging", invoice.getInvoiceId());
                saveLog("LOAD_INVOICE", invoice.getInvoiceId(), "SUCCESS", null, start);
            } else {
                log.warn("Unsupported payload type for quality queue: {}", payload.getClass());
                saveLog("LOAD_UNKNOWN", "unknown", "FAILED", "Unsupported payload type", start);
            }
        } catch (Exception ex) {
            log.error("Error loading payload {}", payload, ex);
            saveLog("LOAD_ERROR", "unknown", "FAILED", ex.getMessage(), start);
        }
    }

    private void saveLog(String jobName, String record, String status, String message, OffsetDateTime start) {
        etlLogRepository.save(EtlLog.builder()
                .jobName(jobName)
                .stage("LOAD")
                .status(status)
                .message(message)
                .sourceRecord(record)
                .startedAt(start)
                .finishedAt(OffsetDateTime.now())
                .build());
    }
}
