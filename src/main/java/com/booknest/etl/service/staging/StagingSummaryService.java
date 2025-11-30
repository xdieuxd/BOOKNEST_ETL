package com.booknest.etl.service.staging;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Qualifier;

@Service
public class StagingSummaryService {

    private final JdbcTemplate stagingJdbcTemplate;

    public StagingSummaryService(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public Map<String, Long> loadSummary() {
        Map<String, Long> summary = new LinkedHashMap<>();
        summary.put("books", countTable("stg_books"));
        summary.put("customers", countTable("stg_customers"));
        summary.put("orders", countTable("stg_orders"));
        summary.put("order_items", countTable("stg_order_items"));
        summary.put("carts", countTable("stg_carts"));
        summary.put("invoices", countTable("stg_invoices"));
        summary.put("dq_passed", countStatus("PASSED"));
        summary.put("dq_failed", countStatus("FAILED"));
        return summary;
    }

    private Long countTable(String table) {
        return stagingJdbcTemplate.queryForObject("SELECT COUNT(*) FROM staging_db." + table, Long.class);
    }

    private Long countStatus(String status) {
        return stagingJdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM staging_db.dq_result WHERE status = ?", Long.class, status);
    }
}
