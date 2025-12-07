package com.booknest.etl.service.dashboard;

import java.time.OffsetDateTime;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class DashboardService {
    public DashboardService(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    private final JdbcTemplate stagingJdbcTemplate;

    public DashboardSummary getSummary() {
        long stagingTotal = count("SELECT COALESCE(SUM(cnt),0) FROM (" +
                "SELECT COUNT(*) cnt FROM staging_db.stg_books " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_customers " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_orders " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_order_items " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_carts " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_invoices" +
                ") AS s");
        
        long passed = count("SELECT COALESCE(SUM(cnt),0) FROM (" +
                "SELECT COUNT(*) cnt FROM staging_db.stg_books WHERE quality_status = 'VALIDATED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_customers WHERE quality_status = 'VALIDATED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_orders WHERE quality_status = 'VALIDATED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_order_items WHERE quality_status = 'VALIDATED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_carts WHERE quality_status = 'VALIDATED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_invoices WHERE quality_status = 'VALIDATED'" +
                ") AS s");
        
        long failed = count("SELECT COALESCE(SUM(cnt),0) FROM (" +
                "SELECT COUNT(*) cnt FROM staging_db.stg_books WHERE quality_status = 'REJECTED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_customers WHERE quality_status = 'REJECTED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_orders WHERE quality_status = 'REJECTED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_order_items WHERE quality_status = 'REJECTED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_carts WHERE quality_status = 'REJECTED' " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_invoices WHERE quality_status = 'REJECTED'" +
                ") AS s");
        
        long fixed = 0;
        
        OffsetDateTime lastRun = null;
        try {
            lastRun = stagingJdbcTemplate.queryForObject(
                "SELECT MAX(loaded_at) FROM (" +
                    "SELECT MAX(loaded_at) loaded_at FROM staging_db.stg_books " +
                    "UNION ALL SELECT MAX(loaded_at) FROM staging_db.stg_customers " +
                    "UNION ALL SELECT MAX(loaded_at) FROM staging_db.stg_orders " +
                    "UNION ALL SELECT MAX(loaded_at) FROM staging_db.stg_order_items " +
                    "UNION ALL SELECT MAX(loaded_at) FROM staging_db.stg_carts " +
                    "UNION ALL SELECT MAX(loaded_at) FROM staging_db.stg_invoices" +
                ") AS t",
                (rs, rowNum) -> {
                    if (rs.getTimestamp(1) == null) {
                        return null;
                    }
                    return rs.getTimestamp(1).toInstant().atOffset(OffsetDateTime.now().getOffset());
                });
        } catch (Exception e) {
            lastRun = OffsetDateTime.now(); // Fallback to current time
        }
        
        return DashboardSummary.builder()
            .totalProcessed(stagingTotal)
            .totalStaging(stagingTotal)
            .passed(passed)
            .fixed(fixed)
            .failed(failed)
            .lastRun(lastRun)
            .build();
        }

    private long count(String sql) {
        Long value = stagingJdbcTemplate.queryForObject(sql, Long.class);
        return value == null ? 0 : value;
    }
}
