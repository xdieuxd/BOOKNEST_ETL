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
        long totalProcessed = count("SELECT COUNT(*) FROM staging_db.dq_result");
        long passed = count("SELECT COUNT(*) FROM staging_db.dq_result WHERE status = 'PASSED'");
        long fixed = count("SELECT COUNT(*) FROM staging_db.dq_result WHERE status = 'FIXED'");
        long failed = count("SELECT COUNT(*) FROM staging_db.dq_result WHERE status = 'FAILED'");
        long stagingTotal = count("SELECT COALESCE(SUM(cnt),0) FROM (" +
                "SELECT COUNT(*) cnt FROM staging_db.stg_books " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_users " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_orders " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_carts " +
                "UNION ALL SELECT COUNT(*) FROM staging_db.stg_invoices" +
                ") AS s");
        OffsetDateTime lastRun = stagingJdbcTemplate.queryForObject("SELECT MAX(finished_at) FROM staging_db.etl_log",
                (rs, rowNum) -> {
                    if (rs.getTimestamp(1) == null) {
                        return null;
                    }
                    return rs.getTimestamp(1).toInstant().atOffset(OffsetDateTime.now().getOffset());
                });
        return DashboardSummary.builder()
            .totalProcessed(totalProcessed)
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
