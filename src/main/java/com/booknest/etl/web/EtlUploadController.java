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

    @PostMapping("/reprocess")
    public ResponseEntity<Map<String, Object>> reprocessData(@RequestBody Map<String, Object> correctedData) {
        try {
            String tracingId = tracingService.generateTracingId();
            log.info("Reprocessing corrected data with tracing ID: {}", tracingId);

            // Convert corrected data back to String map and re-validate (preserve order)
            Map<String, String> rowData = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : correctedData.entrySet()) {
                if (!entry.getKey().startsWith("_")) {
                    rowData.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : "");
                }
            }

            List<Map<String, Object>> transformed = new ArrayList<>();
            List<Map<String, Object>> errors = new ArrayList<>();

            // Re-validate using the same logic as upload (order matters - specific checks first!)
            if (isOrderItemRow(rowData)) {
                processOrderItem(rowData, transformed, errors);
            } else if (isCartRow(rowData)) {
                processCart(rowData, transformed, errors);
            } else if (isInvoiceRow(rowData)) {
                processInvoice(rowData, transformed, errors);
            } else if (isBookRow(rowData)) {
                processBook(rowData, transformed, errors);
            } else if (isCustomerRow(rowData)) {
                processCustomer(rowData, transformed, errors);
            } else if (isOrderRow(rowData)) {
                processOrder(rowData, transformed, errors);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("tracingId", tracingId);
            result.put("status", errors.isEmpty() ? "FIXED" : "STILL_HAS_ERRORS");
            result.put("message", errors.isEmpty() ? "Dữ liệu đã được sửa thành công" : "Dữ liệu vẫn còn lỗi validation");
            result.put("results", Map.of(
                    "transformed", transformed,
                    "errors", errors
            ));

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Error reprocessing data", e);
            return ResponseEntity.badRequest().body(Map.of(
                    "message", "Lỗi khi xử lý lại: " + e.getMessage()
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

            // Persist customer rows when possible and build CSV content for download
            StringBuilder csv = new StringBuilder();
            boolean headerWritten = false;
            java.util.List<String> headerOrder = new java.util.ArrayList<>();

            for (java.util.Map<String, Object> row : rows) {
                // persist customer rows
                java.util.Map<String, String> stringRow = new java.util.LinkedHashMap<>();
                for (java.util.Map.Entry<String, Object> e : row.entrySet()) {
                    if (!e.getKey().startsWith("_")) {
                        stringRow.put(e.getKey(), e.getValue() != null ? e.getValue().toString() : "");
                    }
                }

                if (!headerWritten) {
                    headerOrder.addAll(stringRow.keySet());
                    // write header
                    csv.append(String.join(",", headerOrder));
                    csv.append('\n');
                    headerWritten = true;
                }

                // Data already processed via RabbitMQ - no need to persist here

                // write CSV row (escape commas and quotes)
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
        log.info("ℹ️  Data is automatically processed via RabbitMQ and loaded to source_db");
        log.info("ℹ️  Check RabbitMQ management UI (http://localhost:15672) for queue status");
        
        return ResponseEntity.ok(Map.of(
                "status", "INFO",
                "message", "Dữ liệu tự động được xử lý qua RabbitMQ và load vào DB chính",
                "rabbitmq_ui", "http://localhost:15672",
                "note", "Consumers (RawMessageListener, QualityMessageConsumer) đang xử lý async trong background"
        ));
    }

    private Map<String, Object> processCsvFile(MultipartFile file) throws Exception {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> transformed = new ArrayList<>();
        List<Map<String, Object>> errors = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
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

                // Detect type and process (order matters - specific checks first!)
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

        result.put("extract", Map.of("totalRecords", transformed.size() + errors.size()));
        result.put("dq", Map.of(
                "passed", transformed.size(),
                "failed", errors.size(),
                "fixable", countFixableErrors(errors)
        ));
        result.put("transform", Map.of("processed", transformed.size()));
        result.put("load", Map.of("loaded", transformed.size()));
        result.put("results", Map.of(
                "transformed", transformed,
                "errors", errors
        ));

        return result;
    }

    private boolean isBookRow(Map<String, String> row) {
        // Must have title OR (book_id + price + NOT order_id) to distinguish from order_items
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
        // Store original values before transformation
        String originalTitle = row.getOrDefault("title", "");
        String originalStatus = row.getOrDefault("status", "");
        String originalAuthors = row.getOrDefault("authors", "");
        String priceStr = row.getOrDefault("price", "");
        String releasedAtStr = row.getOrDefault("released_at", "");
        
        // Step 1: Build raw DTO from CSV row
        try {
            // Parse price with better error message
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
            
            // Parse released_at with better error message
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
            
            // Step 1.5: Auto-fix common issues (trim title, uppercase status)
            BookRawMessage fixedBook = autoFixService.fixBook(rawBook);
            
            // Step 2: Validate (synchronous for immediate frontend feedback)
            List<DqErrorDto> validationErrors = dataQualityService.validateBook(fixedBook);
            
            if (validationErrors.isEmpty()) {
                // Step 3: Publish to RabbitMQ raw queue (only if valid)
                messagePublisher.sendRaw(fixedBook);
                log.info("✅ VALIDATION PASSED: Book {} - Sent to RabbitMQ", fixedBook.getBookId());
                
                // Step 4: Transform for frontend preview
                BookRawMessage transformedBook = transformService.transformBook(fixedBook);
                
                // Step 5: Convert to Map for frontend display
                Map<String, Object> processedRow = new LinkedHashMap<>(row);
                processedRow.put("title", transformedBook.getTitle());
                processedRow.put("status", transformedBook.getStatus());
                processedRow.put("authors", String.join(", ", transformedBook.getAuthors()));
                processedRow.put("_original_title", originalTitle);
                processedRow.put("_original_status", originalStatus);
                processedRow.put("_original_authors", originalAuthors);
                transformed.add(processedRow);
            } else {
                // Validation failed - show errors with transform preview
                log.warn("⚠️ VALIDATION FAILED: Book {} - Errors: {}", fixedBook.getBookId(), 
                    validationErrors.stream().map(e -> e.getRule() + ": " + e.getMessage()).toList());
                BookRawMessage transformedBook = transformService.transformBook(fixedBook);
                
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("title", transformedBook.getTitle());
                errorRow.put("status", transformedBook.getStatus());
                errorRow.put("authors", String.join(", ", transformedBook.getAuthors()));
                errorRow.put("_errors", validationErrors);
                errorRow.put("_original_title", originalTitle);
                errorRow.put("_original_status", originalStatus);
                errorRow.put("_original_authors", originalAuthors);
                errors.add(errorRow);
            }
            
        } catch (Exception e) {
            // Parse error (e.g., invalid date/number format)
            log.error("❌ PARSE ERROR: Book {} - {}", row.getOrDefault("book_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            
            // Try to show cleaned values even with parse error
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
        // Store original values before transformation
        String originalFullName = row.getOrDefault("full_name", "");
        String originalEmail = row.getOrDefault("email", "");
        String originalPhone = row.getOrDefault("phone", "");
        String originalStatus = row.getOrDefault("status", "");
        String originalRoles = row.getOrDefault("roles", "");
        
        // Step 1: Build raw DTO from CSV row
        try {
            UserRawMessage rawUser = UserRawMessage.builder()
                .source("csv_upload")
                .userId(row.getOrDefault("customer_id", ""))
                .fullName(originalFullName)
                .email(originalEmail)
                .phone(originalPhone)
                .status(originalStatus)
                .roles(originalRoles.isEmpty() ? Collections.emptyList() : 
                       java.util.Arrays.asList(originalRoles.split("[,|]")))
                .extractedAt(OffsetDateTime.now())
                .build();
            
            // Step 1.5: Auto-fix common issues (phone format, status uppercase, email lowercase)
            UserRawMessage fixedUser = autoFixService.fixUser(rawUser);
            
            // Step 2: Validate (synchronous for immediate frontend feedback)
            List<DqErrorDto> validationErrors = dataQualityService.validateUser(fixedUser);
            
            if (validationErrors.isEmpty()) {
                // Step 3: Publish to RabbitMQ raw queue (only if valid)
                messagePublisher.sendRaw(fixedUser);
                log.info("✅ VALIDATION PASSED: Customer {} - Sent to RabbitMQ", fixedUser.getUserId());
                
                // Step 4: Transform for frontend preview
                UserRawMessage transformedUser = transformService.transformUser(fixedUser);
                
                // Step 5: Convert to Map for frontend display
                Map<String, Object> processedRow = new LinkedHashMap<>(row);
                processedRow.put("full_name", transformedUser.getFullName());
                processedRow.put("email", transformedUser.getEmail());
                processedRow.put("phone", transformedUser.getPhone());
                processedRow.put("status", transformedUser.getStatus());
                processedRow.put("_original_full_name", originalFullName);
                processedRow.put("_original_email", originalEmail);
                processedRow.put("_original_phone", originalPhone);
                processedRow.put("_original_status", originalStatus);
                transformed.add(processedRow);
            } else {
                // Validation failed - show errors with transform preview
                log.warn("⚠️ VALIDATION FAILED: Customer {} - Errors: {}", fixedUser.getUserId(), 
                    validationErrors.stream().map(e -> e.getRule() + ": " + e.getMessage()).toList());
                UserRawMessage transformedUser = transformService.transformUser(fixedUser);
                
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("full_name", transformedUser.getFullName());
                errorRow.put("email", transformedUser.getEmail());
                errorRow.put("phone", transformedUser.getPhone());
                errorRow.put("status", transformedUser.getStatus());
                errorRow.put("_errors", validationErrors);
                errorRow.put("_original_full_name", originalFullName);
                errorRow.put("_original_email", originalEmail);
                errorRow.put("_original_phone", originalPhone);
                errorRow.put("_original_status", originalStatus);
                errors.add(errorRow);
            }
            
        } catch (Exception e) {
            // Parse error
            log.error("❌ PARSE ERROR: Customer {} - {}", row.getOrDefault("customer_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_errors", List.of(DqErrorDto.builder()
                .field("parse_error")
                .rule("PARSE_ERROR")
                .message("Lỗi parse dữ liệu: " + e.getMessage())
                .build()));
            errorRow.put("_original_full_name", originalFullName);
            errorRow.put("_original_email", originalEmail);
            errorRow.put("_original_phone", originalPhone);
            errors.add(errorRow);
        }
    }

    private void processOrder(Map<String, String> row, List<Map<String, Object>> transformed,
                             List<Map<String, Object>> errors) {
        // Store original values before transformation
        String originalCustomerName = row.getOrDefault("customer_name", "");
        String originalCustomerEmail = row.getOrDefault("customer_email", "");
        String originalStatus = row.getOrDefault("status", "");
        String originalPaymentMethod = row.getOrDefault("payment_method", "");
        
        // Step 1: Build raw DTO from CSV row
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
            
            // Step 1.5: Auto-fix common issues (email lowercase, status uppercase)
            OrderRawMessage fixedOrder = autoFixService.fixOrder(rawOrder);
            
            // Step 2: Validate (synchronous for immediate frontend feedback)
            List<DqErrorDto> validationErrors = dataQualityService.validateOrder(fixedOrder);
            
            if (validationErrors.isEmpty()) {
                // Step 3: Publish to RabbitMQ raw queue (only if valid)
                messagePublisher.sendRaw(fixedOrder);
                log.info("✅ VALIDATION PASSED: Order {} - Sent to RabbitMQ", fixedOrder.getOrderId());
                
                // Step 4: Transform for frontend preview
                OrderRawMessage transformedOrder = transformService.transformOrder(fixedOrder);
                
                // Step 5: Convert to Map for frontend display
                Map<String, Object> processedRow = new LinkedHashMap<>(row);
                processedRow.put("customer_name", transformedOrder.getCustomerName());
                processedRow.put("customer_email", transformedOrder.getCustomerEmail());
                processedRow.put("status", transformedOrder.getStatus());
                processedRow.put("payment_method", transformedOrder.getPaymentMethod());
                processedRow.put("_original_customer_name", originalCustomerName);
                processedRow.put("_original_customer_email", originalCustomerEmail);
                processedRow.put("_original_status", originalStatus);
                processedRow.put("_original_payment_method", originalPaymentMethod);
                transformed.add(processedRow);
            } else {
                // Validation failed - show errors with transform preview
                log.warn("⚠️ VALIDATION FAILED: Order {} - Errors: {}", fixedOrder.getOrderId(), 
                    validationErrors.stream().map(e -> e.getRule() + ": " + e.getMessage()).toList());
                OrderRawMessage transformedOrder = transformService.transformOrder(fixedOrder);
                
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("customer_name", transformedOrder.getCustomerName());
                errorRow.put("customer_email", transformedOrder.getCustomerEmail());
                errorRow.put("status", transformedOrder.getStatus());
                errorRow.put("payment_method", transformedOrder.getPaymentMethod());
                errorRow.put("_errors", validationErrors);
                errorRow.put("_original_customer_name", originalCustomerName);
                errorRow.put("_original_customer_email", originalCustomerEmail);
                errorRow.put("_original_status", originalStatus);
                errorRow.put("_original_payment_method", originalPaymentMethod);
                errors.add(errorRow);
            }
        } catch (Exception e) {
            // Parse error - still show transformed/cleaned preview
            log.error("❌ PARSE ERROR: Order {} - {}", row.getOrDefault("order_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            
            // Try to at least normalize what we can
            String cleanedName = originalCustomerName.trim();
            String cleanedEmail = originalCustomerEmail.trim().toLowerCase();
            String cleanedStatus = originalStatus.trim().toUpperCase();
            String cleanedPayment = originalPaymentMethod.trim().toUpperCase();
            
            errorRow.put("customer_name", cleanedName);
            errorRow.put("customer_email", cleanedEmail);
            errorRow.put("status", cleanedStatus);
            errorRow.put("payment_method", cleanedPayment);
            errorRow.put("_errors", List.of(DqErrorDto.builder()
                .field("parse_error")
                .rule("PARSE_ERROR")
                .message("Lỗi parse dữ liệu: " + e.getMessage())
                .build()));
            errorRow.put("_original_customer_name", originalCustomerName);
            errorRow.put("_original_customer_email", originalCustomerEmail);
            errorRow.put("_original_status", originalStatus);
            errorRow.put("_original_payment_method", originalPaymentMethod);
            errors.add(errorRow);
        }
    }

    private void processCart(Map<String, String> row, List<Map<String, Object>> transformed,
                            List<Map<String, Object>> errors) {
        // Store original values before transformation
        String originalCartId = row.getOrDefault("cart_id", "");
        String originalCustomerId = row.getOrDefault("customer_id", "");
        String originalItemBookIds = row.getOrDefault("item_book_ids", "");
        String originalItemQuantities = row.getOrDefault("item_quantities", "");
        
        try {
            // Parse cart items from delimited strings
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
                    // Skip invalid quantity
                }
            }
            
            // Step 1: Build raw DTO from CSV row
            CartRawMessage rawCart = CartRawMessage.builder()
                .source("csv_upload")
                .cartId(originalCartId)
                .customerId(originalCustomerId)
                .items(items)
                .createdAt(parseDateTime(row.get("created_at")))
                .extractedAt(OffsetDateTime.now())
                .build();
            
            // Step 1.5: Auto-fix common issues
            CartRawMessage fixedCart = autoFixService.fixCart(rawCart);
            
            // Step 2: Validate (synchronous for immediate frontend feedback)
            List<DqErrorDto> validationErrors = dataQualityService.validateCart(fixedCart);
            
            if (validationErrors.isEmpty()) {
                // Step 3: Publish to RabbitMQ raw queue (only if valid)
                messagePublisher.sendRaw(fixedCart);
                log.info("✅ VALIDATION PASSED: Cart {} - Sent to RabbitMQ", fixedCart.getCartId());
                
                // Step 4: Transform for frontend preview
                CartRawMessage transformedCart = transformService.transformCart(fixedCart);
                
                // Step 5: Convert to Map for frontend display
                Map<String, Object> processedRow = new LinkedHashMap<>(row);
                processedRow.put("cart_id", transformedCart.getCartId());
                processedRow.put("customer_id", transformedCart.getCustomerId());
                processedRow.put("created_at", transformedCart.getCreatedAt().toString());
                processedRow.put("_original_cart_id", originalCartId);
                processedRow.put("_original_customer_id", originalCustomerId);
                transformed.add(processedRow);
            } else {
                // Validation failed - show errors with transform preview
                log.warn("⚠️ VALIDATION FAILED: Cart {} - Errors: {}", fixedCart.getCartId(), 
                    validationErrors.stream().map(e -> e.getRule() + ": " + e.getMessage()).toList());
                CartRawMessage transformedCart = transformService.transformCart(fixedCart);
                
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("cart_id", transformedCart.getCartId());
                errorRow.put("customer_id", transformedCart.getCustomerId());
                errorRow.put("_errors", validationErrors);
                errorRow.put("_original_cart_id", originalCartId);
                errorRow.put("_original_customer_id", originalCustomerId);
                errors.add(errorRow);
            }
        } catch (Exception e) {
            // Parse error
            log.error("❌ PARSE ERROR: Cart {} - {}", row.getOrDefault("cart_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_errors", List.of(DqErrorDto.builder()
                .field("parse_error")
                .rule("PARSE_ERROR")
                .message("Lỗi parse dữ liệu: " + e.getMessage())
                .build()));
            errorRow.put("_original_cart_id", originalCartId);
            errorRow.put("_original_customer_id", originalCustomerId);
            errors.add(errorRow);
        }
    }

    private void processInvoice(Map<String, String> row, List<Map<String, Object>> transformed,
                                List<Map<String, Object>> errors) {
        // Store original values before transformation
        String originalInvoiceId = row.getOrDefault("invoice_id", "");
        String originalOrderId = row.getOrDefault("order_id", "");
        String originalAmount = row.getOrDefault("amount", "");
        String originalStatus = row.getOrDefault("status", "");
        
        try {
            // Step 1: Build raw DTO from CSV row
            InvoiceRawMessage rawInvoice = InvoiceRawMessage.builder()
                .source("csv_upload")
                .invoiceId(originalInvoiceId)
                .orderId(originalOrderId)
                .amount(originalAmount.isEmpty() ? null : new BigDecimal(originalAmount))
                .status(originalStatus)
                .createdAt(parseDateTime(row.get("issued_at")))
                .extractedAt(OffsetDateTime.now())
                .build();
            
            // Step 1.5: Auto-fix common issues (status uppercase)
            InvoiceRawMessage fixedInvoice = autoFixService.fixInvoice(rawInvoice);
            
            // Step 2: Validate (synchronous for immediate frontend feedback)
            List<DqErrorDto> validationErrors = dataQualityService.validateInvoice(fixedInvoice);
            
            if (validationErrors.isEmpty()) {
                // Step 3: Publish to RabbitMQ raw queue (only if valid)
                messagePublisher.sendRaw(fixedInvoice);
                log.info("✅ VALIDATION PASSED: Invoice {} - Sent to RabbitMQ", fixedInvoice.getInvoiceId());
                
                // Step 4: Transform for frontend preview
                InvoiceRawMessage transformedInvoice = transformService.transformInvoice(fixedInvoice);
                
                // Step 5: Convert to Map for frontend display
                Map<String, Object> processedRow = new LinkedHashMap<>(row);
                processedRow.put("invoice_id", transformedInvoice.getInvoiceId());
                processedRow.put("order_id", transformedInvoice.getOrderId());
                processedRow.put("amount", transformedInvoice.getAmount());
                processedRow.put("status", transformedInvoice.getStatus());
                processedRow.put("_original_invoice_id", originalInvoiceId);
                processedRow.put("_original_order_id", originalOrderId);
                processedRow.put("_original_status", originalStatus);
                transformed.add(processedRow);
            } else {
                // Validation failed - show errors with transform preview
                log.warn("⚠️ VALIDATION FAILED: Invoice {} - Errors: {}", fixedInvoice.getInvoiceId(), 
                    validationErrors.stream().map(e -> e.getRule() + ": " + e.getMessage()).toList());
                InvoiceRawMessage transformedInvoice = transformService.transformInvoice(fixedInvoice);
                
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("invoice_id", transformedInvoice.getInvoiceId());
                errorRow.put("order_id", transformedInvoice.getOrderId());
                errorRow.put("amount", transformedInvoice.getAmount());
                errorRow.put("status", transformedInvoice.getStatus());
                errorRow.put("_errors", validationErrors);
                errorRow.put("_original_invoice_id", originalInvoiceId);
                errorRow.put("_original_order_id", originalOrderId);
                errorRow.put("_original_status", originalStatus);
                errors.add(errorRow);
            }
        } catch (Exception e) {
            // Parse error
            log.error("❌ PARSE ERROR: Invoice {} - {}", row.getOrDefault("invoice_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_errors", List.of(DqErrorDto.builder()
                .field("parse_error")
                .rule("PARSE_ERROR")
                .message("Lỗi parse dữ liệu: " + e.getMessage())
                .build()));
            errorRow.put("_original_invoice_id", originalInvoiceId);
            errorRow.put("_original_order_id", originalOrderId);
            errors.add(errorRow);
        }
    }

    private void processOrderItem(Map<String, String> row, List<Map<String, Object>> transformed,
                                   List<Map<String, Object>> errors) {
        // Store original values before transformation
        String originalOrderId = row.getOrDefault("order_id", "");
        String originalBookId = row.getOrDefault("book_id", "");
        String originalQuantity = row.getOrDefault("quantity", "");
        String originalUnitPrice = row.getOrDefault("unit_price", "");
        
        try {
            // Step 1: Build raw DTO from CSV row
            OrderItemRawMessage rawItem = OrderItemRawMessage.builder()
                .bookId(originalBookId)
                .quantity(originalQuantity.isEmpty() ? null : Integer.parseInt(originalQuantity))
                .unitPrice(originalUnitPrice.isEmpty() ? null : new BigDecimal(originalUnitPrice))
                .build();
            
            // Step 1.5: Auto-fix common issues (trim bookId)
            OrderItemRawMessage fixedItem = autoFixService.fixOrderItem(rawItem);
            
            // Step 2: Validate (synchronous for immediate frontend feedback)
            List<DqErrorDto> validationErrors = dataQualityService.validateOrderItem(fixedItem);
            
            if (validationErrors.isEmpty()) {
                // Step 3: Publish to RabbitMQ raw queue (only if valid)
                messagePublisher.sendRaw(fixedItem);
                log.info("✅ VALIDATION PASSED: OrderItem (order={}, book={}) - Sent to RabbitMQ", originalOrderId, fixedItem.getBookId());
                
                // Step 4: Transform for frontend preview
                OrderItemRawMessage transformedItem = transformService.transformOrderItemPublic(fixedItem);
                
                // Step 5: Convert to Map for frontend display
                Map<String, Object> processedRow = new LinkedHashMap<>(row);
                processedRow.put("order_id", originalOrderId); // Keep order_id as-is
                processedRow.put("book_id", transformedItem.getBookId());
                processedRow.put("quantity", transformedItem.getQuantity());
                processedRow.put("unit_price", transformedItem.getUnitPrice());
                processedRow.put("_original_book_id", originalBookId);
                processedRow.put("_original_quantity", originalQuantity);
                processedRow.put("_original_unit_price", originalUnitPrice);
                transformed.add(processedRow);
            } else {
                // Validation failed - show errors with transform preview
                log.warn("⚠️ VALIDATION FAILED: OrderItem (order={}, book={}) - Errors: {}", originalOrderId, fixedItem.getBookId(), 
                    validationErrors.stream().map(e -> e.getRule() + ": " + e.getMessage()).toList());
                OrderItemRawMessage transformedItem = transformService.transformOrderItemPublic(fixedItem);
                
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("order_id", originalOrderId);
                errorRow.put("book_id", transformedItem.getBookId());
                errorRow.put("quantity", transformedItem.getQuantity());
                errorRow.put("unit_price", transformedItem.getUnitPrice());
                errorRow.put("_errors", validationErrors);
                errorRow.put("_original_book_id", originalBookId);
                errorRow.put("_original_quantity", originalQuantity);
                errorRow.put("_original_unit_price", originalUnitPrice);
                errors.add(errorRow);
            }
        } catch (Exception e) {
            // Parse error
            log.error("❌ PARSE ERROR: OrderItem (order={}, book={}) - {}", 
                row.getOrDefault("order_id", "UNKNOWN"), row.getOrDefault("book_id", "UNKNOWN"), e.getMessage());
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_errors", List.of(DqErrorDto.builder()
                .field("parse_error")
                .rule("PARSE_ERROR")
                .message("Lỗi parse dữ liệu: " + e.getMessage())
                .build()));
            errorRow.put("_original_book_id", originalBookId);
            errorRow.put("_original_quantity", originalQuantity);
            errorRow.put("_original_unit_price", originalUnitPrice);
            errors.add(errorRow);
        }
    }

    private OffsetDateTime parseDateTime(String dateTimeStr) {
        if (dateTimeStr == null || dateTimeStr.isEmpty()) {
            return OffsetDateTime.now();
        }
        
        try {
            // Try parsing with timezone first (ISO-8601 format)
            return OffsetDateTime.parse(dateTimeStr);
        } catch (Exception e) {
            try {
                // Fallback 1: Try ISO LocalDateTime format (with 'T'): 2024-08-01T10:00:00
                return java.time.LocalDateTime.parse(dateTimeStr)
                        .atZone(java.time.ZoneId.systemDefault())
                        .toOffsetDateTime();
            } catch (Exception ex) {
                try {
                    // Fallback 2: Try space-separated format: 2024-08-01 10:00:00
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
        // Count errors that can be auto-fixed
        return (int) errors.stream().filter(e -> {
            String errStr = e.getOrDefault("_errors", "").toString();
            return errStr.contains("NOT_BLANK") || errStr.contains("TRIM");
        }).count();
    }
}
