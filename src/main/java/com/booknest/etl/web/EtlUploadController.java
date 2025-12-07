package com.booknest.etl.web;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.CartItemRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.messaging.producer.EtlMessagePublisher;
import com.booknest.etl.service.dq.DataQualityService;
import com.booknest.etl.service.dq.DataQualityAutoFixService;
import com.booknest.etl.service.transform.TransformService;
import com.booknest.etl.service.tracking.MessageTracingService;
import com.booknest.etl.dto.DqErrorDto;
import java.time.OffsetDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.math.BigDecimal;
import java.util.Collections;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/etl")
@RequiredArgsConstructor
public class EtlUploadController {

    private static final Logger log = LoggerFactory.getLogger(EtlUploadController.class);

    private final EtlMessagePublisher messagePublisher;
    private final DataQualityService dataQualityService;
    private final DataQualityAutoFixService autoFixService;
    private final TransformService transformService;
    private final MessageTracingService tracingService;

    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> uploadCsv(@RequestParam("file") MultipartFile file) {
        try {
            Map<String, Object> result = processCsvFile(file);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Error uploading CSV", e);
            return ResponseEntity.badRequest().body(Map.of(
                    "message", "Lỗi khi xử lý file: " + e.getMessage()
            ));
        }
    }


    @PostMapping("/save")
    public ResponseEntity<?> saveCleaned(@RequestBody Map<String, Object> payload) {
        try {
            Object rowsObj = payload.get("rows");
            if (!(rowsObj instanceof java.util.List)) {
                return ResponseEntity.badRequest().body(Map.of("message", "Invalid payload: missing rows array"));
            }

            @SuppressWarnings("unchecked")
            java.util.List<java.util.Map<String, Object>> rows = (java.util.List<java.util.Map<String, Object>>) rowsObj;

            StringBuilder csv = new StringBuilder();
            boolean headerWritten = false;
            java.util.List<String> headerOrder = new java.util.ArrayList<>();

            for (java.util.Map<String, Object> row : rows) {
                java.util.Map<String, String> stringRow = new java.util.LinkedHashMap<>();
                for (java.util.Map.Entry<String, Object> e : row.entrySet()) {
                    if (!e.getKey().startsWith("_")) {
                        stringRow.put(e.getKey(), e.getValue() != null ? e.getValue().toString() : "");
                    }
                }

                if (!headerWritten) {
                    headerOrder.addAll(stringRow.keySet());
                    csv.append(String.join(",", headerOrder));
                    csv.append('\n');
                    headerWritten = true;
                }

                java.util.List<String> cells = new java.util.ArrayList<>();
                for (String h : headerOrder) {
                    String v = stringRow.getOrDefault(h, "");
                    if (v.contains(",") || v.contains("\"") || v.contains("\n")) {
                        v = "\"" + v.replace("\"", "\"\"") + "\"";
                    }
                    cells.add(v);
                }
                csv.append(String.join(",", cells));
                csv.append('\n');
            }

            byte[] csvBytes = csv.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);

            return ResponseEntity.ok()
                    .header("Content-Disposition", "attachment; filename=cleaned_results.csv")
                    .header("Content-Type", "text/csv; charset=UTF-8")
                    .body(csvBytes);
        } catch (Exception e) {
            log.error("Error saving cleaned data", e);
            return ResponseEntity.status(500).body(Map.of("message", "Lỗi khi lưu dữ liệu: " + e.getMessage()));
        }
    }

    @PostMapping("/load-to-source")
    public ResponseEntity<Map<String, Object>> loadToSourceDb() {
        // This endpoint is no longer needed - data automatically flows to source_db via RabbitMQ
        // RabbitMQ flow: Upload → Raw Queue → Validation → Quality Queue → Transform → Staging → Source DB
        log.info("ℹData is automatically processed via RabbitMQ and loaded to source_db");
        log.info("ℹCheck RabbitMQ management UI (http://localhost:15672) for queue status");
        
        return ResponseEntity.ok(Map.of(
                "status", "INFO",
                "message", "Dữ liệu tự động được xử lý qua RabbitMQ và load vào DB chính",
                "rabbitmq_ui", "http://localhost:15672",
                "note", "Consumers (RawMessageListener, QualityMessageConsumer) đang xử lý async trong background"
        ));
    }

    private Map<String, Object> processCsvFile(MultipartFile file) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            return processCsvContent(content.toString(), file.getOriginalFilename());
        }
    }

    public Map<String, Object> processCsvContent(String csvContent, String fileName) throws Exception {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> transformed = new ArrayList<>();
        List<Map<String, Object>> errors = new ArrayList<>();
        List<Map<String, Object>> raw = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new java.io.StringReader(csvContent))) {
            String line;
            String[] headers = null;
            int lineNum = 0;

            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (lineNum == 1) {
                    headers = line.split(",");
                    continue;
                }

                String[] values = line.split(",");
                if (headers == null || values.length == 0) continue;

                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 0; i < headers.length && i < values.length; i++) {
                    row.put(headers[i].trim(), values[i].trim());
                }
                
                raw.add(new LinkedHashMap<>(row));

                if (isOrderItemRow(row)) {
                    processOrderItem(row, transformed, errors);
                } else if (isCartRow(row)) {
                    processCart(row, transformed, errors);
                } else if (isInvoiceRow(row)) {
                    processInvoice(row, transformed, errors);
                } else if (isBookRow(row)) {
                    processBook(row, transformed, errors);
                } else if (isCustomerRow(row)) {
                    processCustomer(row, transformed, errors);
                } else if (isOrderRow(row)) {
                    processOrder(row, transformed, errors);
                }
            }
        }

        int totalSent = transformed.size();
        
        Map<String, List<Map<String, Object>>> rawByEntity = groupByEntity(raw);
        
        Map<String, List<Map<String, Object>>> transformedByEntity = new HashMap<>();
        Map<String, List<Map<String, Object>>> errorsByEntity = new HashMap<>();
        
        for (String entity : rawByEntity.keySet()) {
            transformedByEntity.put(entity, new ArrayList<>());
            errorsByEntity.put(entity, new ArrayList<>());
        }
        
        result.put("extract", Map.of(
            "totalRecords", totalSent,
            "status", "PROCESSING"
        ));
        result.put("message", String.format("%d records sent to processing pipeline. Click 'Refresh' button to see results after a few seconds.", totalSent));
        result.put("results", Map.of(
                "byEntity", Map.of(
                    "raw", rawByEntity,
                    "transformed", transformedByEntity,
                    "errors", errorsByEntity
                )
        ));
        result.put("dq", Map.of(
            "passed", 0,
            "failed", 0,
            "fixable", 0
        ));
        result.put("tracingId", tracingService.generateTracingId());
        result.put("fileName", fileName);

        return result;
    }

    private boolean isBookRow(Map<String, String> row) {
        return row.containsKey("title") || 
               (row.containsKey("book_id") && row.containsKey("price") && !row.containsKey("order_id"));
    }

    private boolean isCartRow(Map<String, String> row) {
        return row.containsKey("cart_id") || (row.containsKey("item_book_ids") && row.containsKey("item_quantities"));
    }

    private boolean isInvoiceRow(Map<String, String> row) {
        return row.containsKey("invoice_id") || (row.containsKey("order_id") && row.containsKey("amount") && row.containsKey("issued_at"));
    }

    private boolean isOrderItemRow(Map<String, String> row) {
        return (row.containsKey("order_id") && row.containsKey("book_id") && row.containsKey("quantity") && row.containsKey("unit_price"))
                && !row.containsKey("customer_name") && !row.containsKey("customer_email");
    }

    private boolean isCustomerRow(Map<String, String> row) {
        return row.containsKey("customer_id") || row.containsKey("full_name");
    }

    private boolean isOrderRow(Map<String, String> row) {
        return row.containsKey("order_id") || row.containsKey("order_key");
    }

    private void processBook(Map<String, String> row, List<Map<String, Object>> transformed,
                            List<Map<String, Object>> errors) {
        String originalTitle = row.getOrDefault("title", "");
        String originalStatus = row.getOrDefault("status", "");
        String originalAuthors = row.getOrDefault("authors", "");
        String priceStr = row.getOrDefault("price", "");
        String releasedAtStr = row.getOrDefault("released_at", "");
        
        try {
            BigDecimal price = null;
            if (!priceStr.isEmpty()) {
                try {
                    price = new BigDecimal(priceStr);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "Sách có phi phải là giá > 0 - Field 'price' có giá trị không hợp lệ '" + priceStr + 
                        "'. Character a is neither a decimal digit number, decimal point, nor 'e' notation exponential mark."
                    );
                }
            }
            
            LocalDate releasedAt = null;
            if (!releasedAtStr.isEmpty()) {
                try {
                    releasedAt = LocalDate.parse(releasedAtStr);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                        "Field 'released_at' có giá trị không hợp lệ '" + releasedAtStr + 
                        "'. Định dạng hợp lệ: YYYY-MM-DD (ví dụ: 2023-03-15)"
                    );
                }
            }
            
            BookRawMessage rawBook = BookRawMessage.builder()
                .source("csv_upload")
                .bookId(row.getOrDefault("book_id", ""))
                .title(originalTitle)
                .description(row.getOrDefault("description", ""))
                .price(price)
                .free(Boolean.parseBoolean(row.getOrDefault("free_flag", "false")))
                .releasedAt(releasedAt)
                .status(originalStatus)
                .authors(originalAuthors.isEmpty() ? Collections.emptyList() : 
                        java.util.Arrays.asList(originalAuthors.split("[,|]")))
                .categories(row.containsKey("categories") && !row.get("categories").isEmpty() ? 
                           java.util.Arrays.asList(row.get("categories").split("[,|]")) : 
                           Collections.emptyList())
                .extractedAt(OffsetDateTime.now())
                .build();
            
            messagePublisher.sendRaw(rawBook);
            log.info("EXTRACTED: Book {} - Sent to RabbitMQ raw queue", rawBook.getBookId());
            
            Map<String, Object> processedRow = new LinkedHashMap<>(row);
            processedRow.put("_status", "SENT_TO_RABBITMQ");
            transformed.add(processedRow);
            
        } catch (Exception e) {
            log.error("PARSE ERROR: Book {} - {}", row.getOrDefault("book_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            
            String cleanedTitle = originalTitle.trim();
            String cleanedStatus = originalStatus.trim().toUpperCase();
            if (!cleanedTitle.isEmpty()) errorRow.put("title", cleanedTitle);
            if (!cleanedStatus.isEmpty()) errorRow.put("status", cleanedStatus);
            
            errorRow.put("_errors", List.of(DqErrorDto.builder()
                .field("parse_error")
                .rule("PARSE_ERROR")
                .message(e.getMessage() != null ? e.getMessage() : "Lỗi parse dữ liệu")
                .build()));
            errorRow.put("_original_title", originalTitle);
            errorRow.put("_original_status", originalStatus);
            errors.add(errorRow);
        }
    }

    private void processCustomer(Map<String, String> row, List<Map<String, Object>> transformed,
                                List<Map<String, Object>> errors) {
        String originalFullName = row.getOrDefault("full_name", "");
        String originalEmail = row.getOrDefault("email", "");
        String originalPhone = row.getOrDefault("phone", "");
        String originalStatus = row.getOrDefault("status", "");
        String originalRoles = row.getOrDefault("roles", "");
        
        if (originalRoles.isEmpty()) {
            originalRoles = "THANH_VIEN";
        }
        
        try {
            UserRawMessage rawUser = UserRawMessage.builder()
                .source("csv_upload")
                .userId(row.getOrDefault("customer_id", ""))
                .fullName(originalFullName)
                .email(originalEmail)
                .phone(originalPhone)
                .status(originalStatus)
                .roles(java.util.Arrays.asList(originalRoles.split("[,|]")))
                .extractedAt(OffsetDateTime.now())
                .build();
            
            messagePublisher.sendRaw(rawUser);
            log.info("EXTRACTED: Customer {} - Sent to RabbitMQ raw queue", rawUser.getUserId());
            
            Map<String, Object> processedRow = new LinkedHashMap<>(row);
            processedRow.put("_status", "SENT_TO_RABBITMQ");
            transformed.add(processedRow);
            
        } catch (Exception e) {
            log.error("PARSE ERROR: Customer {} - {}", row.getOrDefault("customer_id", "UNKNOWN"), e.getMessage());
        }
    }

    private void processOrder(Map<String, String> row, List<Map<String, Object>> transformed,
                             List<Map<String, Object>> errors) {
        String originalCustomerName = row.getOrDefault("customer_name", "");
        String originalCustomerEmail = row.getOrDefault("customer_email", "");
        String originalStatus = row.getOrDefault("status", "");
        String originalPaymentMethod = row.getOrDefault("payment_method", "");
        
        try {
            String totalAmountStr = row.getOrDefault("total_amount", "");
            String discountStr = row.getOrDefault("discount", "");
            String shippingFeeStr = row.getOrDefault("shipping_fee", "");
            String createdAtStr = row.getOrDefault("created_at", "");
            
            OrderRawMessage rawOrder = OrderRawMessage.builder()
                .source("csv_upload")
                .orderId(row.getOrDefault("order_id", ""))
                .customerName(originalCustomerName)
                .customerEmail(originalCustomerEmail)
                .status(originalStatus)
                .paymentMethod(originalPaymentMethod)
                .totalAmount(totalAmountStr.isEmpty() ? null : new BigDecimal(totalAmountStr))
                .discount(discountStr.isEmpty() ? null : new BigDecimal(discountStr))
                .shippingFee(shippingFeeStr.isEmpty() ? null : new BigDecimal(shippingFeeStr))
                .items(Collections.emptyList())
                .createdAt(parseDateTime(createdAtStr))
                .extractedAt(OffsetDateTime.now())
                .build();
            
            messagePublisher.sendRaw(rawOrder);
            log.info("EXTRACTED: Order {} - Sent to RabbitMQ raw queue", rawOrder.getOrderId());
            
            Map<String, Object> processedRow = new LinkedHashMap<>(row);
            processedRow.put("_status", "SENT_TO_RABBITMQ");
            transformed.add(processedRow);
        } catch (Exception e) {
            log.error("PARSE ERROR: Order {} - {}", row.getOrDefault("order_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_status", "PARSE_ERROR");
            errorRow.put("_error_message", e.getMessage());
            errors.add(errorRow);
        }
    }

    private void processCart(Map<String, String> row, List<Map<String, Object>> transformed,
                            List<Map<String, Object>> errors) {
        String originalCartId = row.getOrDefault("cart_id", "");
        String originalCustomerId = row.getOrDefault("customer_id", "");
        String originalItemBookIds = row.getOrDefault("item_book_ids", "");
        String originalItemQuantities = row.getOrDefault("item_quantities", "");
        
        try {
            String[] bookIds = originalItemBookIds.isEmpty() ? new String[0] : originalItemBookIds.split("\\|");
            String[] quantities = originalItemQuantities.isEmpty() ? new String[0] : originalItemQuantities.split("\\|");
            
            List<CartItemRawMessage> items = new ArrayList<>();
            for (int i = 0; i < Math.min(bookIds.length, quantities.length); i++) {
                try {
                    items.add(CartItemRawMessage.builder()
                            .bookId(bookIds[i].trim())
                            .quantity(Integer.parseInt(quantities[i].trim()))
                            .unitPrice(null) // Price will be populated later
                            .build());
                } catch (NumberFormatException e) {
                }
            }
            
            CartRawMessage rawCart = CartRawMessage.builder()
                .source("csv_upload")
                .cartId(originalCartId)
                .customerId(originalCustomerId)
                .items(items)
                .createdAt(parseDateTime(row.get("created_at")))
                .extractedAt(OffsetDateTime.now())
                .build();
        
            messagePublisher.sendRaw(rawCart);
            log.info("EXTRACTED: Cart {} - Sent to RabbitMQ raw queue", rawCart.getCartId());
            
            Map<String, Object> processedRow = new LinkedHashMap<>(row);
            processedRow.put("_status", "SENT_TO_RABBITMQ");
            transformed.add(processedRow);
        } catch (Exception e) {
            log.error("PARSE ERROR: Cart {} - {}", row.getOrDefault("cart_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_status", "PARSE_ERROR");
            errorRow.put("_error_message", e.getMessage());
            errors.add(errorRow);
        }
    }

    private void processInvoice(Map<String, String> row, List<Map<String, Object>> transformed,
                                List<Map<String, Object>> errors) {
        String originalInvoiceId = row.getOrDefault("invoice_id", "");
        String originalOrderId = row.getOrDefault("order_id", "");
        String originalAmount = row.getOrDefault("amount", "");
        String originalStatus = row.getOrDefault("status", "");
        
        try {
            InvoiceRawMessage rawInvoice = InvoiceRawMessage.builder()
                .source("csv_upload")
                .invoiceId(originalInvoiceId)
                .orderId(originalOrderId)
                .amount(originalAmount.isEmpty() ? null : new BigDecimal(originalAmount))
                .status(originalStatus)
                .issuedAt(parseDateTime(row.get("issued_at")))
                .dueAt(parseDateTime(row.get("due_at")))
                .extractedAt(OffsetDateTime.now())
                .build();
            messagePublisher.sendRaw(rawInvoice);
            log.info("EXTRACTED: Invoice {} - Sent to RabbitMQ raw queue", rawInvoice.getInvoiceId());
            
            Map<String, Object> processedRow = new LinkedHashMap<>(row);
            processedRow.put("_status", "SENT_TO_RABBITMQ");
            transformed.add(processedRow);
        } catch (Exception e) {
            log.error("PARSE ERROR: Invoice {} - {}", row.getOrDefault("invoice_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_status", "PARSE_ERROR");
            errorRow.put("_error_message", e.getMessage());
            errors.add(errorRow);
        }
    }

    private void processOrderItem(Map<String, String> row, List<Map<String, Object>> transformed,
                                   List<Map<String, Object>> errors) {
        String originalOrderId = row.getOrDefault("order_id", "");
        String originalBookId = row.getOrDefault("book_id", "");
        String originalQuantity = row.getOrDefault("quantity", "");
        String originalUnitPrice = row.getOrDefault("unit_price", "");
        
        try {
            OrderItemRawMessage rawItem = OrderItemRawMessage.builder()
                .bookId(originalBookId)
                .quantity(originalQuantity.isEmpty() ? null : Integer.parseInt(originalQuantity))
                .unitPrice(originalUnitPrice.isEmpty() ? null : new BigDecimal(originalUnitPrice))
                .build();
            messagePublisher.sendRaw(rawItem);
            log.info("EXTRACTED: OrderItem (order={}, book={}) - Sent to RabbitMQ raw queue", originalOrderId, rawItem.getBookId());
            
            Map<String, Object> processedRow = new LinkedHashMap<>(row);
            processedRow.put("_status", "SENT_TO_RABBITMQ");
            transformed.add(processedRow);
        } catch (Exception e) {
            log.error("PARSE ERROR: OrderItem (order={}, book={}) - {}", 
                row.getOrDefault("order_id", "UNKNOWN"), row.getOrDefault("book_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_status", "PARSE_ERROR");
            errorRow.put("_error_message", e.getMessage());
            errors.add(errorRow);
        }
    }

    private OffsetDateTime parseDateTime(String dateTimeStr) {
        if (dateTimeStr == null || dateTimeStr.isEmpty()) {
            return OffsetDateTime.now();
        }
        
        try {
            return OffsetDateTime.parse(dateTimeStr);
        } catch (Exception e) {
            try {
                return java.time.LocalDateTime.parse(dateTimeStr)
                        .atZone(java.time.ZoneId.systemDefault())
                        .toOffsetDateTime();
            } catch (Exception ex) {
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    return java.time.LocalDateTime.parse(dateTimeStr, formatter)
                            .atZone(java.time.ZoneId.systemDefault())
                            .toOffsetDateTime();
                } catch (Exception ex2) {
                    log.warn("Failed to parse datetime '{}', using current time", dateTimeStr);
                    return OffsetDateTime.now();
                }
            }
        }
    }

    private int countFixableErrors(List<Map<String, Object>> errors) {
        return (int) errors.stream().filter(e -> {
            String errStr = e.getOrDefault("_errors", "").toString();
            return errStr.contains("NOT_BLANK") || errStr.contains("TRIM");
        }).count();
    }

    private String convertToEntityKey(String detectedType) {
        switch (detectedType) {
            case "BOOK": return "books";
            case "CUSTOMER": return "customers";
            case "ORDER": return "orders";
            case "ORDER_ITEM": return "order_items";
            case "CART": return "carts";
            case "INVOICE": return "invoices";
            default: return "unknown";
        }
    }
    

    private Map<String, List<Map<String, Object>>> groupByEntity(List<Map<String, Object>> rawData) {
        Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
        
        for (Map<String, Object> row : rawData) {
            String entityType = detectEntityType(row);
            grouped.computeIfAbsent(entityType, k -> new ArrayList<>()).add(row);
        }
        
        return grouped;
    }

    private String detectEntityType(Map<String, Object> row) {
        Map<String, String> stringRow = new HashMap<>();
        row.forEach((k, v) -> stringRow.put(k, v != null ? v.toString() : ""));
        
        if (isOrderItemRow(stringRow)) return "order_items";
        if (isCartRow(stringRow)) return "carts";
        if (isInvoiceRow(stringRow)) return "invoices";
        if (isBookRow(stringRow)) return "books";
        if (isCustomerRow(stringRow)) return "customers";
        if (isOrderRow(stringRow)) return "orders";
        
        return "unknown";
    }
}
