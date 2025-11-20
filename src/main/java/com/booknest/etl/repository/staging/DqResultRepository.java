package com.booknest.etl.repository.staging;

import java.sql.Types;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class DqResultRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public DqResultRepository(JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void saveResult(String entityType, String entityKey, DataQualityStatus status, String errorsJson) {
        String sql = """
                INSERT INTO dq_result (entity_type, entity_key, status, errors, checked_at)
                VALUES (?, ?, ?, ?, NOW())
                """;
        stagingJdbcTemplate.update(sql,
                new Object[]{entityType, entityKey, status != null ? status.value() : null, errorsJson},
                new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.LONGVARCHAR});
    }
}
