package com.booknest.etl.repository.staging;

import java.sql.Types;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Qualifier;

import com.booknest.etl.dto.CartItemRawMessage;
import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class StagingCartItemRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public StagingCartItemRepository(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void replaceItems(String cartKey, List<CartItemRawMessage> items) {
        stagingJdbcTemplate.update("DELETE FROM staging_db.stg_cart_items WHERE cart_key = ?", cartKey);
        if (items == null || items.isEmpty()) {
            return;
        }
        String sql = """
                INSERT INTO staging_db.stg_cart_items (cart_key, book_key, quantity, unit_price, quality_status, quality_errors, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, NOW())
                """;
        for (CartItemRawMessage item : items) {
            stagingJdbcTemplate.update(sql, new Object[]{
                    cartKey,
                    item.getBookId(),
                    item.getQuantity(),
                    item.getUnitPrice(),
                    DataQualityStatus.VALIDATED.value(),
                    null
            }, new int[]{Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.DECIMAL, Types.VARCHAR, Types.LONGVARCHAR});
        }
    }
}
