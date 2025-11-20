package com.booknest.etl.service.dashboard;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class TransformedDataService {

    private final JdbcTemplate stagingJdbcTemplate;

    public List<Map<String, Object>> getTransformed(String entity) {
        return switch (entity.toUpperCase()) {
            case "BOOK", "BOOKS" -> query("SELECT * FROM stg_books ORDER BY loaded_at DESC LIMIT 100");
            case "CUSTOMER", "CUSTOMERS" -> query("SELECT * FROM stg_customers ORDER BY loaded_at DESC LIMIT 100");
            case "ORDER", "ORDERS" -> query("SELECT * FROM stg_orders ORDER BY loaded_at DESC LIMIT 100");
            case "CART", "CARTS" -> query("SELECT * FROM stg_carts ORDER BY loaded_at DESC LIMIT 100");
            case "INVOICE", "INVOICES" -> query("SELECT * FROM stg_invoices ORDER BY loaded_at DESC LIMIT 100");
            default -> List.of();
        };
    }

    public List<Map<String, Object>> getErrors() {
        return query("SELECT entity_type, entity_key, status, errors, checked_at FROM dq_result ORDER BY checked_at DESC LIMIT 100");
    }

    private List<Map<String, Object>> query(String sql) {
        return stagingJdbcTemplate.queryForList(sql);
    }
}
