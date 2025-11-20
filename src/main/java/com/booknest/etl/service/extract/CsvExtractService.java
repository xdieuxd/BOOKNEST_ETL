package com.booknest.etl.service.extract;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class CsvExtractService {

    private static final Logger log = LoggerFactory.getLogger(CsvExtractService.class);

    private final ResourceLoader resourceLoader;

    public List<BookRawMessage> readBooks() {
        return readBooksFromResource("classpath:data/source/books_source.csv");
    }

    public List<UserRawMessage> readCustomers() {
        return readCustomersFromResource("classpath:data/source/customers_source.csv");
    }

    public List<OrderRawMessage> readOrders() {
        return readOrdersFromResource(
                "classpath:data/source/orders_source.csv",
                "classpath:data/source/order_items_source.csv"
        );
    }

    private List<BookRawMessage> readBooksFromResource(String location) {
        Resource resource = resourceLoader.getResource(location);
        List<BookRawMessage> result = new ArrayList<>();
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {
            for (CSVRecord record : parser) {
                result.add(BookRawMessage.builder()
                        .source("csv")
                        .bookId(record.get("book_id"))
                        .title(record.get("title"))
                        .description(record.get("description"))
                        .price(parseBigDecimal(record.get("price")))
                        .free(Boolean.parseBoolean(record.get("free_flag")))
                        .releasedAt(parseDate(record.get("released_at")))
                        .status("HIEU_LUC")
                        .authors(splitToList(record.get("authors")))
                        .categories(splitToList(record.get("categories")))
                        .extractedAt(OffsetDateTime.now())
                        .build());
            }
        } catch (IOException ex) {
            log.error("Cannot read CSV books from {}", location, ex);
        }
        return result;
    }

    private List<UserRawMessage> readCustomersFromResource(String location) {
        Resource resource = resourceLoader.getResource(location);
        List<UserRawMessage> result = new ArrayList<>();
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {
            for (CSVRecord record : parser) {
                result.add(UserRawMessage.builder()
                        .source("csv")
                        .userId(record.get("customer_id"))
                        .fullName(record.get("full_name"))
                        .email(record.get("email"))
                        .phone(record.get("phone"))
                        .status(record.get("status"))
                        .roles(splitToList(record.get("roles")))
                        .extractedAt(OffsetDateTime.now())
                        .build());
            }
        } catch (IOException ex) {
            log.error("Cannot read CSV customers from {}", location, ex);
        }
        return result;
    }

    private List<OrderRawMessage> readOrdersFromResource(String ordersLocation, String orderItemsLocation) {
        Map<String, List<OrderItemRawMessage>> itemsByOrder = readOrderItems(orderItemsLocation);
        Resource resource = resourceLoader.getResource(ordersLocation);
        List<OrderRawMessage> result = new ArrayList<>();
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {
            for (CSVRecord record : parser) {
                String orderId = record.get("order_id");
                result.add(OrderRawMessage.builder()
                        .source("csv")
                        .orderId(orderId)
                        .customerName(record.get("customer_name"))
                        .customerEmail(record.get("customer_email"))
                        .status(record.get("status"))
                        .paymentMethod(record.get("payment_method"))
                        .totalAmount(parseBigDecimal(record.get("total_amount")))
                        .discount(parseBigDecimal(record.get("discount")))
                        .shippingFee(parseBigDecimal(record.get("shipping_fee")))
                        .items(itemsByOrder.getOrDefault(orderId, List.of()))
                        .createdAt(parseDateTime(record.get("created_at")))
                        .extractedAt(OffsetDateTime.now())
                        .build());
            }
        } catch (IOException ex) {
            log.error("Cannot read CSV orders from {}", ordersLocation, ex);
        }
        return result;
    }

    private Map<String, List<OrderItemRawMessage>> readOrderItems(String location) {
        Resource resource = resourceLoader.getResource(location);
        Map<String, List<OrderItemRawMessage>> result = new HashMap<>();
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {
            for (CSVRecord record : parser) {
                String orderId = record.get("order_id");
                result.computeIfAbsent(orderId, k -> new ArrayList<>())
                        .add(OrderItemRawMessage.builder()
                                .bookId(record.get("book_id"))
                                .quantity(parseInt(record.get("quantity")))
                                .unitPrice(parseBigDecimal(record.get("unit_price")))
                                .build());
            }
        } catch (IOException ex) {
            log.error("Cannot read CSV order items from {}", location, ex);
        }
        return result;
    }

    private List<String> splitToList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        String normalized = raw.replace("|", ",").replace(";", ",");
        String[] array = normalized.split(",");
        List<String> result = new ArrayList<>();
        for (String element : array) {
            String trimmed = element.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    private BigDecimal parseBigDecimal(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return new BigDecimal(raw.trim());
        } catch (NumberFormatException ex) {
            log.warn("Cannot parse BigDecimal value '{}'", raw);
            return null;
        }
    }

    private LocalDate parseDate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(raw);
        } catch (DateTimeParseException ex) {
            log.warn("Cannot parse date value '{}'", raw);
            return null;
        }
    }

    private OffsetDateTime parseDateTime(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(raw);
        } catch (DateTimeParseException ex) {
            // try without zone
            try {
                return java.time.LocalDateTime.parse(raw)
                        .atOffset(OffsetDateTime.now().getOffset());
            } catch (DateTimeParseException second) {
                log.warn("Cannot parse datetime value '{}'", raw);
                return null;
            }
        }
    }

    private Integer parseInt(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            log.warn("Cannot parse integer value '{}'", raw);
            return null;
        }
    }
}
