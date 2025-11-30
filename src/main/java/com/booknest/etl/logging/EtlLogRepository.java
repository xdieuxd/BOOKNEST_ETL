package com.booknest.etl.logging;

import java.sql.Types;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Qualifier;

@Repository
public class EtlLogRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public EtlLogRepository(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void save(EtlLog log) {
        String sql = """
                INSERT INTO etl_log (job_name, stage, status, message, started_at, finished_at, source_record, target_record)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;
        stagingJdbcTemplate.update(sql,
                new Object[]{
                        log.getJobName(),
                        log.getStage(),
                        log.getStatus(),
                        log.getMessage(),
                        log.getStartedAt(),
                        log.getFinishedAt(),
                        log.getSourceRecord(),
                        log.getTargetRecord()
                },
                new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.LONGVARCHAR,
                        Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR});
    }
}
