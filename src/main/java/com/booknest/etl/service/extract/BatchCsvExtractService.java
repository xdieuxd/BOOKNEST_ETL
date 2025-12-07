package com.booknest.etl.service.extract;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Service;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartItemRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.messaging.producer.BookMessageProducer;
import com.booknest.etl.messaging.producer.CartMessageProducer;
import com.booknest.etl.messaging.producer.CustomerMessageProducer;
import com.booknest.etl.messaging.producer.InvoiceMessageProducer;
import com.booknest.etl.messaging.producer.OrderItemMessageProducer;
import com.booknest.etl.messaging.producer.OrderMessageProducer;

import lombok.RequiredArgsConstructor;


@Service
@RequiredArgsConstructor
public class BatchCsvExtractService {

    private static final Logger log = LoggerFactory.getLogger(BatchCsvExtractService.class);

    private final BookMessageProducer bookProducer;
    private final CustomerMessageProducer customerProducer;
    private final OrderMessageProducer orderProducer;
    private final OrderItemMessageProducer orderItemProducer;
    private final CartMessageProducer cartProducer;
    private final InvoiceMessageProducer invoiceProducer;


    public void extractAllCsvFiles() {
        log.info("Starting batch CSV extraction...");

        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources("classpath:data/source/*_source.csv");

            log.info("Found {} CSV files", resources.length);

            for (Resource resource : resources) {
                String filename = resource.getFilename();
                log.info("Processing file: {}", filename);

                if (filename == null) {
                    log.warn("Skipping resource with null filename");
                    continue;
                }

                if (filename.startsWith("books_")) {
                    extractBooks(resource);
                } else if (filename.startsWith("customers_")) {
                    extractCustomers(resource);
                } else if (filename.startsWith("orders_")) {
                    extractOrders(resource);
                } else if (filename.startsWith("order_items_")) {
                    extractOrderItems(resource);
                } else if (filename.startsWith("carts_")) {
                    extractCarts(resource);
                } else if (filename.startsWith("invoices_")) {
                    extractInvoices(resource);
                } else {
                    log.warn("Skipping unsupported file: {}", filename);
                }
            }

            log.info("Batch CSV extraction completed");

        } catch (Exception e) {
            log.error("Error during batch CSV extraction: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to extract CSV files", e);
        }
    }

    private void extractBooks(Resource resource) throws Exception {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {

            reader.readLine(); // skip header
            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    String[] values = line.split(",", -1);

                    BookRawMessage book = BookRawMessage.builder()
                        .source("batch_csv_extract")
                        .bookId(values[0])
                        .title(values[1])
                        .authors(values[2].isEmpty() ? Collections.emptyList() :
                                Arrays.asList(values[2].split("[|,]")))
                        .categories(values[3].isEmpty() ? Collections.emptyList() :
                                   Arrays.asList(values[3].split("[|,]")))
                        .price(values[4].isEmpty() ? null : new BigDecimal(values[4]))
                        .releasedAt(values[5].isEmpty() ? null : LocalDate.parse(values[5]))
                        .free(Boolean.parseBoolean(values[6]))
                        .status(values[7])
                        .description(values.length > 8 ? values[8] : "")
                        .extractedAt(OffsetDateTime.now())
                        .build();

                    bookProducer.sendToRaw(book);
                    count++;

                } catch (Exception e) {
                    log.warn("Failed to parse book line: {} - {}", line, e.getMessage());
                }
            }
        }
        log.info("Extracted {} books from {}", count, resource.getFilename());
    }

    private void extractCustomers(Resource resource) throws Exception {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {

            reader.readLine(); // skip header
            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    String[] values = line.split(",", -1);

                    UserRawMessage customer = UserRawMessage.builder()
                        .source("batch_csv_extract")
                        .userId(values[0])
                        .fullName(values[1])
                        .email(values[2])
                        .phone(values[3])
                        .status(values[4])
                        .roles(values[5].isEmpty() ? Collections.emptyList() :
                              Arrays.asList(values[5].split("[|,]")))
                        .extractedAt(OffsetDateTime.now())
                        .build();

                    customerProducer.sendToRaw(customer);
                    count++;

                } catch (Exception e) {
                    log.warn("Failed to parse customer line: {} - {}", line, e.getMessage());
                }
            }
        }
        log.info("Extracted {} customers from {}", count, resource.getFilename());
    }

    private void extractOrders(Resource resource) throws Exception {
        Map<String, List<OrderItemRawMessage>> itemsByOrder = readOrderItemsForOrders();
        
        int count = 0;
        int errors = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {

            reader.readLine(); 
            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    String[] values = line.split(",", -1);
                    String orderId = values[0];

                    BigDecimal totalAmount = values[5].isEmpty() ? BigDecimal.ZERO : new BigDecimal(values[5]);
                    BigDecimal discount = values[6].isEmpty() ? BigDecimal.ZERO : new BigDecimal(values[6]);
                    BigDecimal shippingFee = values[7].isEmpty() ? BigDecimal.ZERO : new BigDecimal(values[7]);

                    OrderRawMessage order = OrderRawMessage.builder()
                        .source("batch_csv_extract")
                        .orderId(orderId)
                        .customerName(values[1])
                        .customerEmail(values[2])
                        .status(values[3])
                        .paymentMethod(values[4])
                        .totalAmount(totalAmount)
                        .discount(discount)
                        .shippingFee(shippingFee)
                        .items(itemsByOrder.getOrDefault(orderId, Collections.emptyList()))
                        .createdAt(values[8].isEmpty() ? OffsetDateTime.now() :
                                   LocalDateTime.parse(values[8]).atOffset(java.time.ZoneOffset.UTC))
                        .extractedAt(OffsetDateTime.now())
                        .build();

                    orderProducer.sendToRaw(order);
                    count++;

                } catch (Exception e) {
                    errors++;
                    log.error("Failed to parse order line #{}: {} - Error: {}", errors, line, e.getMessage(), e);
                }
            }
        }
        log.info("Extracted {} orders from {} ({} errors)", count, resource.getFilename(), errors);
    }
    
    private Map<String, List<OrderItemRawMessage>> readOrderItemsForOrders() throws Exception {
        Map<String, List<OrderItemRawMessage>> result = new HashMap<>();
        
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources("classpath:data/source/order_items_source.csv");
            
            if (resources.length == 0) {
                log.warn("No order_items_source.csv found, orders will have empty items");
                return result;
            }
            
            Resource resource = resources[0];
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {

                reader.readLine(); // skip header
                String line;

                while ((line = reader.readLine()) != null) {
                    try {
                        String[] values = line.split(",", -1);
                        String orderId = values[0];
                        
                        OrderItemRawMessage item = OrderItemRawMessage.builder()
                            .bookId(values[1])
                            .quantity(values[2].isEmpty() ? 0 : Integer.parseInt(values[2]))
                            .unitPrice(values[3].isEmpty() ? BigDecimal.ZERO : new BigDecimal(values[3]))
                            .build();
                        
                        result.computeIfAbsent(orderId, k -> new ArrayList<>()).add(item);
                        
                    } catch (Exception e) {
                        log.warn("Failed to parse order item line: {} - {}", line, e.getMessage());
                    }
                }
            }
            
            log.info("Loaded {} order items for {} orders", 
                result.values().stream().mapToInt(List::size).sum(), result.size());
            
        } catch (Exception e) {
            log.error("Error reading order_items_source.csv: {}", e.getMessage(), e);
        }
        
        return result;
    }

    private void extractOrderItems(Resource resource) throws Exception {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {

            reader.readLine(); // header
            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    String[] values = line.split(",", -1);

                    OrderItemRawMessage item = OrderItemRawMessage.builder()
                        .bookId(values[1])
                        .quantity(values[2].isEmpty() ? null : Integer.parseInt(values[2]))
                        .unitPrice(values[3].isEmpty() ? null : new BigDecimal(values[3]))
                        .build();

                    orderItemProducer.sendToRaw(item);
                    count++;

                } catch (Exception e) {
                    log.warn("Failed to parse order item line: {} - {}", line, e.getMessage());
                }
            }
        }
        log.info("Extracted {} order items from {}", count, resource.getFilename());
    }

    private void extractCarts(Resource resource) throws Exception {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {

            reader.readLine(); // header
            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    String[] values = line.split(",", -1);

                    String[] bookIds = values[2].isEmpty() ? new String[0] : values[2].split("\\|");
                    String[] quantities = values[3].isEmpty() ? new String[0] : values[3].split("\\|");

                    List<CartItemRawMessage> items = new ArrayList<>();
                    for (int i = 0; i < Math.min(bookIds.length, quantities.length); i++) {
                        items.add(CartItemRawMessage.builder()
                                .bookId(bookIds[i].trim())
                                .quantity(Integer.parseInt(quantities[i].trim()))
                                .unitPrice(null)
                                .build());
                    }

                    CartRawMessage cart = CartRawMessage.builder()
                        .source("batch_csv_extract")
                        .cartId(values[0])
                        .customerId(values[1])
                        .items(items)
                        .createdAt(values[4].isEmpty() ? OffsetDateTime.now() :
                                   LocalDateTime.parse(values[4]).atOffset(java.time.ZoneOffset.UTC))
                        .extractedAt(OffsetDateTime.now())
                        .build();

                    cartProducer.sendToRaw(cart);
                    count++;

                } catch (Exception e) {
                    log.warn("Failed to parse cart line: {} - {}", line, e.getMessage());
                }
            }
        }
        log.info("Extracted {} carts from {}", count, resource.getFilename());
    }

    private void extractInvoices(Resource resource) throws Exception {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {

            reader.readLine(); // header
            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    String[] values = line.split(",", -1);

                    InvoiceRawMessage invoice = InvoiceRawMessage.builder()
                        .source("batch_csv_extract")
                        .invoiceId(values[0])
                        .orderId(values[1])
                        .amount(values[2].isEmpty() ? null : new BigDecimal(values[2]))
                        .status(values[3])
                        .issuedAt(values[4].isEmpty() ? OffsetDateTime.now() :
                                  LocalDateTime.parse(values[4]).atOffset(java.time.ZoneOffset.UTC))
                        .dueAt(values.length > 5 && !values[5].isEmpty() ?
                               LocalDateTime.parse(values[5]).atOffset(java.time.ZoneOffset.UTC) : null)
                        .extractedAt(OffsetDateTime.now())
                        .build();

                    invoiceProducer.sendToRaw(invoice);
                    count++;

                } catch (Exception e) {
                    log.warn("Failed to parse invoice line: {} - {}", line, e.getMessage());
                }
            }
        }
        log.info("Extracted {} invoices from {}", count, resource.getFilename());
    }
}
