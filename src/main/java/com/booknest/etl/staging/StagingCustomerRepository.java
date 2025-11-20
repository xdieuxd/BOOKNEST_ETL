package com.booknest.etl.staging;

import java.sql.Types;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class StagingCustomerRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public StagingCustomerRepository(JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void upsert(UserRawMessage user, DataQualityStatus qualityStatus, String errors) {
        String sql = """
                INSERT INTO stg_customers (customer_key, full_name, email, phone, roles, status, quality_status, quality_errors, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    full_name = VALUES(full_name),
                    email = VALUES(email),
                    phone = VALUES(phone),
                    roles = VALUES(roles),
                    status = VALUES(status),
                    quality_status = VALUES(quality_status),
                    quality_errors = VALUES(quality_errors),
                    loaded_at = NOW()
                """;
        stagingJdbcTemplate.update(sql, new Object[]{
                user.getUserId(),
                user.getFullName(),
                user.getEmail(),
                user.getPhone(),
                String.join(", ", user.getRoles()),
                user.getStatus(),
                qualityStatus != null ? qualityStatus.value() : null,
                errors
        }, new int[]{
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.LONGVARCHAR
        });
    }
}
