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
        return query("SELECT entity_type, entity_key, status, errors, checked_at FROM staging_db.dq_result WHERE status = 'FAILED' ORDER BY checked_at DESC LIMIT 100");
    }

    public Map<String, Object> getTransformedByEntity() {
        return Map.of(
            "books", query("SELECT * FROM staging_db.stg_books WHERE quality_status = 'VALIDATED' ORDER BY loaded_at DESC LIMIT 100"),
            "customers", query("SELECT * FROM staging_db.stg_customers WHERE quality_status = 'VALIDATED' ORDER BY loaded_at DESC LIMIT 100"),
            "orders", query("SELECT * FROM staging_db.stg_orders WHERE quality_status = 'VALIDATED' ORDER BY order_date DESC LIMIT 100"),
            "order_items", query("SELECT * FROM staging_db.stg_order_items WHERE quality_status = 'VALIDATED' ORDER BY loaded_at DESC LIMIT 100"),
            "carts", query("SELECT * FROM staging_db.stg_carts WHERE quality_status = 'VALIDATED' ORDER BY created_at DESC LIMIT 100"),
            "invoices", query("SELECT * FROM staging_db.stg_invoices WHERE quality_status = 'VALIDATED' ORDER BY issued_at DESC LIMIT 100")
        );
    }

    public Map<String, Object> getErrorsByEntity() {
        return Map.of(
            "books", query("SELECT * FROM staging_db.stg_books WHERE quality_status = 'REJECTED' ORDER BY loaded_at DESC LIMIT 100"),
            "customers", query("SELECT * FROM staging_db.stg_customers WHERE quality_status = 'REJECTED' ORDER BY loaded_at DESC LIMIT 100"),
            "orders", query("SELECT * FROM staging_db.stg_orders WHERE quality_status = 'REJECTED' ORDER BY order_date DESC LIMIT 100"),
            "order_items", query("SELECT * FROM staging_db.stg_order_items WHERE quality_status = 'REJECTED' ORDER BY loaded_at DESC LIMIT 100"),
            "carts", query("SELECT * FROM staging_db.stg_carts WHERE quality_status = 'REJECTED' ORDER BY created_at DESC LIMIT 100"),
            "invoices", query("SELECT * FROM staging_db.stg_invoices WHERE quality_status = 'REJECTED' ORDER BY issued_at DESC LIMIT 100")
        );
    }

    public Map<String, Object> getRawByEntity() {
        // Raw = all data that went through RawConsumer (both VALIDATED and REJECTED)
        return Map.of(
            "books", query("SELECT * FROM staging_db.stg_books ORDER BY loaded_at DESC LIMIT 100"),
            "customers", query("SELECT * FROM staging_db.stg_customers ORDER BY loaded_at DESC LIMIT 100"),
            "orders", query("SELECT * FROM staging_db.stg_orders ORDER BY order_date DESC LIMIT 100"),
            "order_items", query("SELECT * FROM staging_db.stg_order_items ORDER BY loaded_at DESC LIMIT 100"),
            "carts", query("SELECT * FROM staging_db.stg_carts ORDER BY created_at DESC LIMIT 100"),
            "invoices", query("SELECT * FROM staging_db.stg_invoices ORDER BY issued_at DESC LIMIT 100")
        );
    }

    private List<Map<String, Object>> query(String sql) {
        return stagingJdbcTemplate.queryForList(sql);
    }
}
