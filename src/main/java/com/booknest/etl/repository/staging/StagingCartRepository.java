package com.booknest.etl.repository.staging;

import java.sql.Types;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Qualifier;

import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class StagingCartRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public StagingCartRepository(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void upsert(CartRawMessage cart, DataQualityStatus status, String errors) {
        String sql = """
                INSERT INTO staging_db.stg_carts (cart_key, customer_key, created_at, quality_status, quality_errors, loaded_at)
                VALUES (?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    customer_key = VALUES(customer_key),
                    created_at = VALUES(created_at),
                    quality_status = VALUES(quality_status),
                    quality_errors = VALUES(quality_errors),
                    loaded_at = NOW()
                """;
        stagingJdbcTemplate.update(sql, new Object[]{
                cart.getCartId(),
                cart.getCustomerId(),
                cart.getCreatedAt(),
                status != null ? status.value() : null,
                errors
        }, new int[]{Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.LONGVARCHAR});
    }
}
