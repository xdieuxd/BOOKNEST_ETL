package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.repository.staging.StagingBookRepository;
import com.booknest.etl.repository.staging.StagingCartRepository;
import com.booknest.etl.repository.staging.StagingInvoiceRepository;
import com.booknest.etl.repository.staging.StagingOrderItemRepository;
import com.booknest.etl.repository.staging.StagingOrderRepository;
import com.booknest.etl.service.load.SourceDbLoaderService;
import com.booknest.etl.service.transform.TransformService;
import com.booknest.etl.staging.StagingCustomerRepository;

import lombok.RequiredArgsConstructor;


@Component
@RequiredArgsConstructor
public class QualityMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(QualityMessageConsumer.class);

    private final StagingCustomerRepository stagingCustomerRepository;
    private final StagingBookRepository stagingBookRepository;
    private final StagingOrderRepository stagingOrderRepository;
    private final StagingCartRepository stagingCartRepository;
    private final StagingInvoiceRepository stagingInvoiceRepository;
    private final StagingOrderItemRepository stagingOrderItemRepository;
    private final TransformService transformService;
    private final SourceDbLoaderService sourceDbLoaderService;

    @RabbitHandler
    @Transactional
    public void handleCustomer(UserRawMessage rawCustomer) {
        try {
            // Transform: normalize names, emails, etc.
            UserRawMessage transformed = transformService.transformUser(rawCustomer);
            
            log.debug("Saving validated customer {} to staging_db", transformed.getUserId());
            stagingCustomerRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            
            // Load to source_db immediately 
            sourceDbLoaderService.loadCustomersToSource();
            
            log.info("Customer {} processed: quality queue → transform → staging → source_db", transformed.getUserId());
        } catch (Exception e) {
            log.error("Error processing customer {}: {}", rawCustomer.getUserId(), e.getMessage(), e);
        }
    }

    @RabbitHandler
    @Transactional
    public void handleBook(BookRawMessage rawBook) {
        try {
            // Transform: capitalize titles, normalize author names, etc.
            BookRawMessage transformed = transformService.transformBook(rawBook);
            
            log.debug("Saving validated book {} to staging_db", transformed.getBookId());
            stagingBookRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            
            sourceDbLoaderService.loadBooksToSource();
            
            log.info("Book {} processed: quality queue → transform → staging → source_db", transformed.getBookId());
        } catch (Exception e) {
            log.error("Error processing book {}: {}", rawBook.getBookId(), e.getMessage(), e);
        }
    }

    @RabbitHandler
    @Transactional
    public void handleOrder(OrderRawMessage rawOrder) {
        try {
            // Transform: normalize customer names, recalculate totals, etc.
            OrderRawMessage transformed = transformService.transformOrder(rawOrder);
            
            log.debug("Saving validated order {} to staging_db", transformed.getOrderId());
            stagingOrderRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            
            sourceDbLoaderService.loadOrdersToSource();
            
            log.info("Order {} processed: quality queue → transform → staging → source_db", transformed.getOrderId());
        } catch (Exception e) {
            log.error("Error processing order {}: {}", rawOrder.getOrderId(), e.getMessage(), e);
        }
    }

    @RabbitHandler
    @Transactional
    public void handleCart(CartRawMessage rawCart) {
        try {
            // Transform: trim IDs, normalize cart data
            CartRawMessage transformed = transformService.transformCart(rawCart);
            
            log.debug("Saving validated cart {} to staging_db", transformed.getCartId());
            stagingCartRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            
            log.info("Cart {} processed: quality queue → transform → staging_db", transformed.getCartId());
            // TODO: Implement loadCartsToSource() when needed
        } catch (Exception e) {
            log.error("Error processing cart {}: {}", rawCart.getCartId(), e.getMessage(), e);
        }
    }

    @RabbitHandler
    @Transactional
    public void handleInvoice(InvoiceRawMessage rawInvoice) {
        try {
            // Transform: trim, uppercase status, etc.
            InvoiceRawMessage transformed = transformService.transformInvoice(rawInvoice);
            
            log.debug("Saving validated invoice {} to staging_db", transformed.getInvoiceId());
            stagingInvoiceRepository.upsert(transformed, DataQualityStatus.VALIDATED, null);
            
            log.info("Invoice {} processed: quality queue → transform → staging_db", transformed.getInvoiceId());
            // TODO: Implement loadInvoicesToSource() when needed
        } catch (Exception e) {
            log.error("Error processing invoice {}: {}", rawInvoice.getInvoiceId(), e.getMessage(), e);
        }
    }

    @RabbitHandler
    @Transactional
    public void handleOrderItem(OrderItemRawMessage rawOrderItem) {
        try {
            // Transform: trim book IDs, etc.
            OrderItemRawMessage transformed = transformService.transformOrderItemPublic(rawOrderItem);
            
            log.debug("Saving validated order item (book={}) to staging_db", transformed.getBookId());
            
            log.info("Order item (book={}) processed: quality queue → transform → staging_db", transformed.getBookId());
            // Order items are typically loaded together with orders
        } catch (Exception e) {
            log.error("Error processing order item (book={}): {}", rawOrderItem.getBookId(), e.getMessage(), e);
        }
    }
}
