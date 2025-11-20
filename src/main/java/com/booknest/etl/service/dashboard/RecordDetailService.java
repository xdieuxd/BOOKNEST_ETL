package com.booknest.etl.service.dashboard;

import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.repository.book.BookJdbcRepository;
import com.booknest.etl.repository.cart.CartJdbcRepository;
import com.booknest.etl.repository.invoice.InvoiceJdbcRepository;
import com.booknest.etl.repository.user.UserJdbcRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class RecordDetailService {

    private final BookJdbcRepository bookJdbcRepository;
    private final UserJdbcRepository userJdbcRepository;
    private final CartJdbcRepository cartJdbcRepository;
    private final InvoiceJdbcRepository invoiceJdbcRepository;
    private final JdbcTemplate stagingJdbcTemplate;
    private final ObjectMapper objectMapper;

    public RecordDetailResponse getRecordDetail(String entityType, String entityKey) {
        Map<String, Object> raw = fetchRaw(entityType, entityKey);
        Map<String, Object> staging = fetchStaging(entityType, entityKey);
        Map<String, Object> dq = fetchDqResult(entityType, entityKey);
        return RecordDetailResponse.builder()
                .entityType(entityType)
                .entityKey(entityKey)
                .rawData(raw)
                .stagingData(staging)
                .dqResult(dq)
                .build();
    }

    private Map<String, Object> fetchRaw(String entityType, String entityKey) {
        try {
            return switch (entityType.toUpperCase()) {
                case "BOOK" -> bookJdbcRepository.findById(entityKey).map(this::convert).orElse(Map.of());
                case "CUSTOMER", "USER" -> userJdbcRepository.findById(entityKey).map(this::convert).orElse(Map.of());
                case "CART" -> cartJdbcRepository.findById(entityKey).map(this::convert).orElse(Map.of());
                case "INVOICE" -> invoiceJdbcRepository.findById(entityKey).map(this::convert).orElse(Map.of());
                default -> Map.of();
            };
        } catch (Exception ex) {
            return Map.of();
        }
    }

    private Map<String, Object> fetchStaging(String entityType, String entityKey) {
        try {
            return switch (entityType.toUpperCase()) {
                case "BOOK" -> stagingJdbcTemplate.queryForMap("SELECT * FROM stg_books WHERE book_key = ?", entityKey);
                case "CUSTOMER", "USER" -> stagingJdbcTemplate.queryForMap("SELECT * FROM stg_customers WHERE customer_key = ?", entityKey);
                case "ORDER" -> stagingJdbcTemplate.queryForMap("SELECT * FROM stg_orders WHERE order_key = ?", entityKey);
                case "CART" -> stagingJdbcTemplate.queryForMap("SELECT * FROM stg_carts WHERE cart_key = ?", entityKey);
                case "INVOICE" -> stagingJdbcTemplate.queryForMap("SELECT * FROM stg_invoices WHERE invoice_key = ?", entityKey);
                default -> Map.of();
            };
        } catch (Exception ex) {
            return Map.of();
        }
    }

    private Map<String, Object> fetchDqResult(String entityType, String entityKey) {
        try {
            return stagingJdbcTemplate.queryForMap("""
                    SELECT status, errors, checked_at
                    FROM dq_result
                    WHERE entity_type = ? AND entity_key = ?
                    ORDER BY checked_at DESC
                    LIMIT 1
                    """, entityType, entityKey);
        } catch (Exception ex) {
            return Map.of();
        }
    }

    private Map<String, Object> convert(Object dto) {
        return objectMapper.convertValue(dto, Map.class);
    }
}
