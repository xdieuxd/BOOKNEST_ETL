package com.booknest.etl.repository.staging;

import java.sql.Types;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Qualifier;

import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class DqResultRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public DqResultRepository(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void saveResult(String entityType, String entityKey, DataQualityStatus status, String errorsJson) {
        String sql = """
            INSERT INTO staging_db.dq_result (entity_type, entity_key, status, errors, checked_at)
            VALUES (?, ?, ?, ?, NOW())
            """;
        stagingJdbcTemplate.update(sql,
                new Object[]{entityType, entityKey, status != null ? status.value() : null, errorsJson},
                new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.LONGVARCHAR});
    }
}
