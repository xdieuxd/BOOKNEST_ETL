package com.booknest.etl.repository.staging;

import java.sql.Types;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Qualifier;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class StagingBookRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public StagingBookRepository(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void upsert(BookRawMessage message, DataQualityStatus qualityStatus, String qualityErrors) {
        String sql = """
                INSERT INTO staging_db.stg_books (book_key, title, authors, categories, description, price, free_flag,
                                       released_at, avg_rating, total_orders, quality_status, quality_errors, source, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    authors = VALUES(authors),
                    categories = VALUES(categories),
                    description = VALUES(description),
                    price = VALUES(price),
                    free_flag = VALUES(free_flag),
                    released_at = VALUES(released_at),
                    avg_rating = VALUES(avg_rating),
                    total_orders = VALUES(total_orders),
                    quality_status = VALUES(quality_status),
                    quality_errors = VALUES(quality_errors),
                    source = VALUES(source),
                    loaded_at = NOW()
                """;
        Object[] params = {
                message.getBookId(),
                message.getTitle(),
                String.join(", ", message.getAuthors()),
                String.join(", ", message.getCategories()),
                message.getDescription(),
                message.getPrice(),
                message.isFree(),
                message.getReleasedAt(),
                message.getAverageRating(),
                message.getTotalOrders(),
                qualityStatus != null ? qualityStatus.value() : null,
                qualityErrors,
                message.getSource()
        };
        int[] types = {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.LONGVARCHAR, Types.DECIMAL, Types.BOOLEAN, Types.DATE,
                Types.DECIMAL, Types.INTEGER, Types.VARCHAR, Types.LONGVARCHAR,
                Types.VARCHAR
        };
        stagingJdbcTemplate.update(sql, params, types);
    }
}
