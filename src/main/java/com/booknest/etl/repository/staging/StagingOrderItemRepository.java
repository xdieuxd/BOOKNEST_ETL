package com.booknest.etl.repository.staging;

import java.sql.Types;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.beans.factory.annotation.Qualifier;

import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class StagingOrderItemRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public StagingOrderItemRepository(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void upsert(OrderItemRawMessage item, DataQualityStatus status, String errors) {
        String sql = """
                INSERT INTO staging_db.stg_order_items 
                (order_key, book_key, quantity, unit_price, quality_status, quality_errors, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    quantity = VALUES(quantity),
                    unit_price = VALUES(unit_price),
                    quality_status = VALUES(quality_status),
                    quality_errors = VALUES(quality_errors),
                    loaded_at = NOW()
                """;
        
        String orderKey = "ORDER_" + (item.getBookId() != null ? item.getBookId() : "UNKNOWN");
        
        stagingJdbcTemplate.update(sql, new Object[]{
                orderKey,
                item.getBookId(),
                item.getQuantity(),
                item.getUnitPrice(),
                status.value(),
                errors
        }, new int[]{
                Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.DECIMAL,
                Types.VARCHAR, Types.LONGVARCHAR
        });
    }

    public void replaceItems(String orderKey, List<OrderItemRawMessage> items) {
        stagingJdbcTemplate.update("DELETE FROM staging_db.stg_order_items WHERE order_key = ?", orderKey);
        if (items == null || items.isEmpty()) {
            return;
        }
        String sql = """
                INSERT INTO staging_db.stg_order_items (order_key, book_key, quantity, unit_price, quality_status, quality_errors, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, NOW())
                """;
        for (OrderItemRawMessage item : items) {
            stagingJdbcTemplate.update(sql, new Object[]{
                    orderKey,
                    item.getBookId(),
                    item.getQuantity(),
                    item.getUnitPrice(),
                    DataQualityStatus.VALIDATED.value(),
                    null
            }, new int[]{
                    Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.DECIMAL,
                    Types.VARCHAR, Types.LONGVARCHAR
            });
        }
    }
}
