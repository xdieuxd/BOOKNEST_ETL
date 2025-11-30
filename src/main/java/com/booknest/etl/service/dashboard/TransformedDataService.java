package com.booknest.etl.service.dashboard;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Qualifier;

@Service
public class TransformedDataService {

    private final JdbcTemplate stagingJdbcTemplate;

    public TransformedDataService(@Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public List<Map<String, Object>> getTransformed(String entity) {
        return switch (entity.toUpperCase()) {
            case "BOOK", "BOOKS" ->
                    query("SELECT * FROM staging_db.stg_books ORDER BY loaded_at DESC LIMIT 100");
            case "CUSTOMER", "CUSTOMERS" ->
                    query("SELECT * FROM staging_db.stg_customers ORDER BY loaded_at DESC LIMIT 100");
            case "ORDER", "ORDERS" ->
                    query("SELECT * FROM staging_db.stg_orders ORDER BY order_date DESC LIMIT 100");
            case "CART", "CARTS" ->
                    query("SELECT * FROM staging_db.stg_carts ORDER BY created_at DESC LIMIT 100");
            case "INVOICE", "INVOICES" ->
                    query("SELECT * FROM staging_db.stg_invoices ORDER BY created_at DESC LIMIT 100");
            default -> List.of();
        };
    }

    public List<Map<String, Object>> getErrors() {
        return query("SELECT entity_type, entity_key, status, errors, checked_at FROM staging_db.dq_result ORDER BY checked_at DESC LIMIT 100");
    }

    private List<Map<String, Object>> query(String sql) {
        return stagingJdbcTemplate.queryForList(sql);
    }
}
