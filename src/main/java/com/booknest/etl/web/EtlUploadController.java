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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.messaging.producer.EtlMessagePublisher;
import com.booknest.etl.service.dq.DataQualityService;
import com.booknest.etl.service.dq.DataQualityAutoFixService;
import com.booknest.etl.service.transform.TransformService;
import com.booknest.etl.service.tracking.MessageTracingService;
import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.staging.StagingCustomerRepository;
import com.booknest.etl.repository.staging.StagingBookRepository;
import com.booknest.etl.repository.staging.StagingOrderRepository;
import com.booknest.etl.dq.DataQualityStatus;
import com.booknest.etl.service.load.SourceDbLoaderService;
import java.time.OffsetDateTime;
import java.time.LocalDate;
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
    private final ObjectMapper objectMapper;
    private final StagingCustomerRepository stagingCustomerRepository;
    private final StagingBookRepository stagingBookRepository;
    private final StagingOrderRepository stagingOrderRepository;
    private final SourceDbLoaderService sourceDbLoaderService;

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

            // Re-validate using the same logic as upload
            if (isBookRow(rowData)) {
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

                // persist if customer
                if (isCustomerRow(stringRow)) {
                    com.booknest.etl.dto.UserRawMessage user = com.booknest.etl.dto.UserRawMessage.builder()
                            .source(stringRow.getOrDefault("source", "presentation-ui"))
                            .userId(stringRow.getOrDefault("customer_id", stringRow.getOrDefault("userId", stringRow.getOrDefault("user_id", ""))))
                            .fullName(stringRow.getOrDefault("full_name", stringRow.getOrDefault("fullName", stringRow.getOrDefault("full_name", ""))))
                            .email(stringRow.getOrDefault("email", ""))
                            .phone(stringRow.getOrDefault("phone", ""))
                            .status(stringRow.getOrDefault("status", ""))
                            .roles(stringRow.getOrDefault("roles", "").isEmpty() ? java.util.List.of() : java.util.Arrays.stream(stringRow.getOrDefault("roles", "").split(",")).map(String::trim).toList())
                            .build();

                    String errors = row.getOrDefault("_errors", "").toString();
                    stagingCustomerRepository.upsert(user, DataQualityStatus.FIXED, errors);
                }

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
        try {
            log.info("🚀 Loading validated data from staging_db to source_db...");
            log.info("📋 Load order: 1️⃣ Customers → 2️⃣ Books → 3️⃣ Orders (dependency order)");
            
            // Load in correct dependency order to avoid FK violations
            log.info("1️⃣ Loading customers...");
            int customersLoaded = sourceDbLoaderService.loadCustomersToSource();
            log.info("✅ {} customers loaded", customersLoaded);
            
            log.info("2️⃣ Loading books...");
            int booksLoaded = sourceDbLoaderService.loadBooksToSource();
            log.info("✅ {} books loaded", booksLoaded);
            
            log.info("3️⃣ Loading orders...");
            int ordersLoaded = sourceDbLoaderService.loadOrdersToSource();
            log.info("✅ {} orders loaded", ordersLoaded);
            
            int totalLoaded = customersLoaded + booksLoaded + ordersLoaded;
            String message = String.format(
                "Đã load thành công %d records vào DB chính (Customers: %d, Books: %d, Orders: %d)",
                totalLoaded, customersLoaded, booksLoaded, ordersLoaded
            );
            
            Map<String, Object> result = new HashMap<>();
            result.put("status", "SUCCESS");
            result.put("message", message);
            result.put("loaded", Map.of(
                    "customers", customersLoaded,
                    "books", booksLoaded,
                    "orders", ordersLoaded,
                    "total", totalLoaded
            ));
            
            log.info("🎉 Load completed: {} total records to source_db", totalLoaded);
            
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Error loading data to source_db", e);
            return ResponseEntity.status(500).body(Map.of(
                    "status", "ERROR",
                    "message", "Lỗi khi load dữ liệu vào DB chính: " + e.getMessage()
            ));
        }
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

                // Detect type and process
                if (isBookRow(row)) {
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
        return row.containsKey("book_id") || row.containsKey("title");
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
        String price = row.getOrDefault("price", "");
        
        // Step 1: Build raw DTO from CSV row
        try {
            BookRawMessage rawBook = BookRawMessage.builder()
                .source("csv_upload")
                .bookId(row.getOrDefault("book_id", ""))
                .title(originalTitle)
                .description(row.getOrDefault("description", ""))
                .price(price.isEmpty() ? null : new BigDecimal(price))
                .free(Boolean.parseBoolean(row.getOrDefault("free_flag", "false")))
                .releasedAt(row.containsKey("released_at") && !row.get("released_at").isEmpty() ? 
                           LocalDate.parse(row.get("released_at")) : null)
                .status(originalStatus)
                .authors(originalAuthors.isEmpty() ? Collections.emptyList() : 
                        java.util.Arrays.asList(originalAuthors.split("[,|]")))
                .categories(row.containsKey("categories") && !row.get("categories").isEmpty() ? 
                           java.util.Arrays.asList(row.get("categories").split("[,|]")) : 
                           Collections.emptyList())
                .extractedAt(OffsetDateTime.now())
                .build();
            
            // Step 2: Validate using DataQualityService
            List<DqErrorDto> dqErrors = dataQualityService.validateBook(rawBook);
            
            if (dqErrors.isEmpty()) {
                // Step 3: Transform (normalize names, uppercase status)
                BookRawMessage transformedBook = transformService.transformBook(rawBook);
                
                // Step 4: Convert to Map for frontend display
                Map<String, Object> processedRow = new LinkedHashMap<>(row);
                processedRow.put("title", transformedBook.getTitle());
                processedRow.put("status", transformedBook.getStatus());
                processedRow.put("authors", String.join(", ", transformedBook.getAuthors()));
                processedRow.put("_original_title", originalTitle);
                processedRow.put("_original_status", originalStatus);
                processedRow.put("_original_authors", originalAuthors);
                transformed.add(processedRow);
                
                // Step 5: Save to staging_db
                try {
                    stagingBookRepository.upsert(transformedBook, DataQualityStatus.VALIDATED, null);
                } catch (Exception e) {
                    log.error("Error saving book to staging: {}", e.getMessage());
                }
            } else {
                // Validation failed - return errors
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("_errors", dqErrors);
                errorRow.put("_original_title", originalTitle);
                errorRow.put("_original_status", originalStatus);
                errorRow.put("_original_authors", originalAuthors);
                errors.add(errorRow);
            }
        } catch (Exception e) {
            // Parse error (e.g., invalid date/number format)
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_errors", List.of(DqErrorDto.builder()
                .field("parse_error")
                .rule("PARSE_ERROR")
                .message("Lỗi parse dữ liệu: " + e.getMessage())
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
            
            // Step 2: Validate using DataQualityService
            List<DqErrorDto> dqErrors = dataQualityService.validateUser(rawUser);
            
            if (dqErrors.isEmpty()) {
                // Step 3: Transform (normalize names, lowercase email, clean phone)
                UserRawMessage transformedUser = transformService.transformUser(rawUser);
                
                // Step 4: Convert to Map for frontend display
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
                
                // Step 5: Save to staging_db
                try {
                    stagingCustomerRepository.upsert(transformedUser, DataQualityStatus.VALIDATED, null);
                } catch (Exception e) {
                    log.error("Error saving customer to staging: {}", e.getMessage());
                }
            } else {
                // Validation failed - return errors
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("_errors", dqErrors);
                errorRow.put("_original_full_name", originalFullName);
                errorRow.put("_original_email", originalEmail);
                errorRow.put("_original_phone", originalPhone);
                errorRow.put("_original_status", originalStatus);
                errors.add(errorRow);
            }
        } catch (Exception e) {
            // Parse error
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
                .createdAt(createdAtStr.isEmpty() ? OffsetDateTime.now() : OffsetDateTime.parse(createdAtStr))
                .extractedAt(OffsetDateTime.now())
                .build();
            
            // Step 2: Validate using DataQualityService
            List<DqErrorDto> dqErrors = dataQualityService.validateOrder(rawOrder);
            
            if (dqErrors.isEmpty()) {
                // Step 3: Transform (normalize name, lowercase email, uppercase status)
                OrderRawMessage transformedOrder = transformService.transformOrder(rawOrder);
                
                // Step 4: Convert to Map for frontend display
                Map<String, Object> processedRow = new LinkedHashMap<>(row);
                processedRow.put("customer_name", transformedOrder.getCustomerName());
                processedRow.put("customer_email", transformedOrder.getCustomerEmail());
                processedRow.put("status", transformedOrder.getStatus());
                processedRow.put("payment_method", transformedOrder.getPaymentMethod());
                processedRow.put("_original_customer_name", originalCustomerName);
                processedRow.put("_original_customer_email", originalCustomerEmail);
                processedRow.put("_original_status", originalStatus);
                transformed.add(processedRow);
                
                // Step 5: Save to staging_db
                try {
                    stagingOrderRepository.upsert(transformedOrder, DataQualityStatus.VALIDATED, null);
                } catch (Exception e) {
                    log.error("Error saving order to staging: {}", e.getMessage());
                }
            } else {
                // Validation failed - return errors
                Map<String, Object> errorRow = new LinkedHashMap<>(row);
                errorRow.put("_errors", dqErrors);
                errorRow.put("_original_customer_name", originalCustomerName);
                errorRow.put("_original_customer_email", originalCustomerEmail);
                errorRow.put("_original_status", originalStatus);
                errors.add(errorRow);
            }
        } catch (Exception e) {
            // Parse error
            Map<String, Object> errorRow = new LinkedHashMap<>(row);
            errorRow.put("_errors", List.of(DqErrorDto.builder()
                .field("parse_error")
                .rule("PARSE_ERROR")
                .message("Lỗi parse dữ liệu: " + e.getMessage())
                .build()));
            errorRow.put("_original_customer_name", originalCustomerName);
            errorRow.put("_original_customer_email", originalCustomerEmail);
            errorRow.put("_original_status", originalStatus);
            errors.add(errorRow);
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
