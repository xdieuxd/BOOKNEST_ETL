package com.booknest.etl.web;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.booknest.etl.service.extract.BatchCsvExtractService;
import com.booknest.etl.web.EtlUploadController;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/etl")
@RequiredArgsConstructor
public class BatchProcessController {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessController.class);

    private final BatchCsvExtractService batchCsvExtractService;
    private final JdbcTemplate stagingJdbcTemplate;
    private final EtlUploadController etlUploadController;

    /**
     * Trigger batch CSV extraction for all entity types.
     * Reads all CSV files từ classpath:data/source/ và send to RabbitMQ raw queues.
     * 
     * @return Success message with extraction summary
     */
    @PostMapping("/batch-extract")
    public ResponseEntity<String> batchExtract() {
        log.info("Batch extract endpoint called");
        
        try {
            batchCsvExtractService.extractAllCsvFiles();
            
            String message = "Batch CSV extraction completed successfully. " +
                           "Check RabbitMQ queues for processing status.";
            
            return ResponseEntity.ok(message);
            
        } catch (Exception e) {
            log.error("Batch extraction failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body("Batch extraction failed: " + e.getMessage());
        }
    }

    @PostMapping("/batch-extract-with-results")
    public ResponseEntity<Map<String, Object>> batchExtractWithResults() throws InterruptedException {
        log.info("Batch extract with results endpoint called");
        
        try {
            batchCsvExtractService.extractAllCsvFiles();
            log.info("Waiting 5 seconds for RabbitMQ processing...");
            Thread.sleep(5000);
            
            List<Map<String, Object>> allRecords = new ArrayList<>();
            
            String bookSql = "SELECT 'BOOK' as entity_type, book_key, quality_status, quality_errors, " +
                           "title, authors, price, categories FROM staging_db.stg_books";
            allRecords.addAll(stagingJdbcTemplate.queryForList(bookSql));
            
            String customerSql = "SELECT 'CUSTOMER' as entity_type, customer_key, quality_status, quality_errors, " +
                               "full_name, email, phone FROM staging_db.stg_customers";
            allRecords.addAll(stagingJdbcTemplate.queryForList(customerSql));
             
            String orderSql = "SELECT 'ORDER' as entity_type, order_key, quality_status, quality_errors, " +
                            "customer_key, order_date, total_amount FROM staging_db.stg_orders";
            allRecords.addAll(stagingJdbcTemplate.queryForList(orderSql));
            
            Map<String, List<Map<String, Object>>> rawByEntity = new HashMap<>();
            Map<String, List<Map<String, Object>>> transformedByEntity = new HashMap<>();
            Map<String, List<Map<String, Object>>> errorsByEntity = new HashMap<>();
            
            rawByEntity.put("books", new ArrayList<>());
            rawByEntity.put("customers", new ArrayList<>());
            rawByEntity.put("orders", new ArrayList<>());
            transformedByEntity.put("books", new ArrayList<>());
            transformedByEntity.put("customers", new ArrayList<>());
            transformedByEntity.put("orders", new ArrayList<>());
            errorsByEntity.put("books", new ArrayList<>());
            errorsByEntity.put("customers", new ArrayList<>());
            errorsByEntity.put("orders", new ArrayList<>());
            
            int totalPassed = 0;
            int totalFailed = 0;
            
            for (Map<String, Object> record : allRecords) {
                String entityType = ((String) record.get("entity_type")).toLowerCase();
                String entityKey = entityType.equals("customer") ? "customers" : 
                                 entityType.equals("order") ? "orders" : "books";
                String status = (String) record.get("quality_status");
                
                rawByEntity.get(entityKey).add(record);
                
                if ("VALIDATED".equalsIgnoreCase(status)) {
                    transformedByEntity.get(entityKey).add(record);
                    totalPassed++;
                } else if ("REJECTED".equalsIgnoreCase(status)) {
                    record.put("_errors", record.get("quality_errors"));
                    errorsByEntity.get(entityKey).add(record);
                    totalFailed++;
                }
            }
            
            Map<String, Object> response = new HashMap<>();
            Map<String, Object> results = new HashMap<>();
            Map<String, Object> byEntity = new HashMap<>();
            byEntity.put("raw", rawByEntity);
            byEntity.put("transformed", transformedByEntity);
            byEntity.put("errors", errorsByEntity);
            results.put("byEntity", byEntity);
            
            response.put("results", results);
            
            int totalExtracted = allRecords.size();
            response.put("extract", Map.of(
                "totalRecords", totalExtracted
            ));
            
            response.put("transform", Map.of(
                "processed", totalPassed
            ));
            
            response.put("load", Map.of(
                "loaded", totalPassed
            ));
            
            response.put("dq", Map.of(
                "passed", totalPassed,
                "failed", totalFailed,
                "fixable", 0
            ));
            response.put("tracingId", "batch-" + System.currentTimeMillis());
            response.put("fileName", "Auto Batch Extract (All CSV Files)");
            
            log.info("Batch extract completed: {} transformed, {} errors", 
                    totalPassed, totalFailed);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Batch extraction with results failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/batch-process-sync")
    public ResponseEntity<Map<String, Object>> batchProcessSync() {
        log.info("Starting batch process (sync) for all CSV files from classpath");
        
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            String[] csvPatterns = {
                "classpath:data/source/books_source.csv",
                "classpath:data/source/customers_source.csv",
                "classpath:data/source/orders_source.csv",
                "classpath:data/source/order_items_source.csv",
                "classpath:data/source/carts_source.csv",
                "classpath:data/source/invoices_source.csv"
            };
            
            Map<String, List<Map<String, Object>>> rawByEntity = new LinkedHashMap<>();
            Map<String, List<Map<String, Object>>> transformedByEntity = new LinkedHashMap<>();
            Map<String, List<Map<String, Object>>> errorsByEntity = new LinkedHashMap<>();
            Map<String, Map<String, Integer>> dqByEntity = new LinkedHashMap<>();
            
            int totalPassed = 0;
            int totalFailed = 0;
            int totalFixable = 0;
            
            for (String pattern : csvPatterns) {
                try {
                    Resource resource = resolver.getResource(pattern);
                    if (!resource.exists()) {
                        log.warn("CSV file not found: {}", pattern);
                        continue;
                    }
                    
                    String fileName = resource.getFilename();
                    String entityType = extractEntityType(fileName);
                    
                    log.info("Uploading CSV file: {} (entity: {})", fileName, entityType);
                  
                    Map<String, Object> result = uploadCsvFromResource(resource);

                    @SuppressWarnings("unchecked")
                    Map<String, Object> results = (Map<String, Object>) result.get("results");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> dq = (Map<String, Object>) result.get("dq");
                    
                    if (results != null && results.containsKey("byEntity")) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> byEntity = (Map<String, Object>) results.get("byEntity");
                        
                        @SuppressWarnings("unchecked")
                        Map<String, List<Map<String, Object>>> rawMap = (Map<String, List<Map<String, Object>>>) byEntity.get("raw");
                        @SuppressWarnings("unchecked")
                        Map<String, List<Map<String, Object>>> transformedMap = (Map<String, List<Map<String, Object>>>) byEntity.get("transformed");
                        @SuppressWarnings("unchecked")
                        Map<String, List<Map<String, Object>>> errorsMap = (Map<String, List<Map<String, Object>>>) byEntity.get("errors");
                        
                        if (rawMap != null) {
                            rawMap.forEach((entity, data) -> {
                                rawByEntity.computeIfAbsent(entity, k -> new ArrayList<>()).addAll(data);
                            });
                        }
                        if (transformedMap != null) {
                            transformedMap.forEach((entity, data) -> {
                                transformedByEntity.computeIfAbsent(entity, k -> new ArrayList<>()).addAll(data);
                            });
                        }
                        if (errorsMap != null) {
                            errorsMap.forEach((entity, data) -> {
                                errorsByEntity.computeIfAbsent(entity, k -> new ArrayList<>()).addAll(data);
                            });
                        }
                    }
                    
                    if (dq != null) {
                        int passed = (Integer) dq.getOrDefault("passed", 0);
                        int failed = (Integer) dq.getOrDefault("failed", 0);
                        int fixable = (Integer) dq.getOrDefault("fixable", 0);
                        
                        Map<String, Integer> entityDq = new LinkedHashMap<>();
                        entityDq.put("passed", passed);
                        entityDq.put("failed", failed);
                        entityDq.put("fixable", fixable);
                        dqByEntity.put(entityType, entityDq);
                        
                        totalPassed += passed;
                        totalFailed += failed;
                        totalFixable += fixable;
                    }
                    
                } catch (Exception e) {
                    log.error("Failed to process {}: {}", pattern, e.getMessage());
                }
            }
            
            Map<String, Object> response = new LinkedHashMap<>();
            Map<String, Object> results = new LinkedHashMap<>();
            results.put("byEntity", Map.of(
                "raw", rawByEntity,
                "transformed", transformedByEntity,
                "errors", errorsByEntity
            ));
            results.put("dqByEntity", dqByEntity);
            response.put("results", results);
            
            Map<String, Object> dq = new LinkedHashMap<>();
            dq.put("passed", totalPassed);
            dq.put("failed", totalFailed);
            dq.put("fixable", totalFixable);
            response.put("dq", dq);
            
            response.put("tracingId", "batch-sync-" + System.currentTimeMillis());
            response.put("fileName", "All CSV Files (Auto-Extracted)");
            response.put("entityTypes", new ArrayList<>(rawByEntity.keySet()));
            
            log.info("Batch sync processing completed: {} entities, total passed={}, failed={}, fixable={}", 
                    rawByEntity.size(), totalPassed, totalFailed, totalFixable);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Batch sync processing failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    private Map<String, Object> uploadCsvFromResource(Resource resource) throws Exception {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
            
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
            
            return etlUploadController.processCsvContent(content.toString(), resource.getFilename());
        }
    }

    private String extractEntityType(String fileName) {
        if (fileName == null) return "unknown";
        
        String lower = fileName.toLowerCase();
        if (lower.contains("book")) return "books";
        if (lower.contains("customer")) return "customers";
        if (lower.contains("order_item")) return "order_items";
        if (lower.contains("order")) return "orders";
        if (lower.contains("cart")) return "carts";
        if (lower.contains("invoice")) return "invoices";
        
        return "unknown";
    }
}
