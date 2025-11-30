package com.booknest.etl.repository.staging;

import java.sql.Types;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Qualifier;

import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class StagingInvoiceRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public StagingInvoiceRepository(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void upsert(InvoiceRawMessage invoice, DataQualityStatus status, String errors) {
        String sql = """
                INSERT INTO staging_db.stg_invoices (invoice_key, order_key, amount, status, created_at, quality_status, quality_errors, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    order_key = VALUES(order_key),
                    amount = VALUES(amount),
                    status = VALUES(status),
                    created_at = VALUES(created_at),
                    quality_status = VALUES(quality_status),
                    quality_errors = VALUES(quality_errors),
                    loaded_at = NOW()
                """;
        stagingJdbcTemplate.update(sql, new Object[]{
                invoice.getInvoiceId(),
                invoice.getOrderId(),
                invoice.getAmount(),
                invoice.getStatus(),
                invoice.getCreatedAt(),
                status != null ? status.value() : null,
                errors
        }, new int[]{Types.VARCHAR, Types.VARCHAR, Types.DECIMAL, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.LONGVARCHAR});
    }
}
