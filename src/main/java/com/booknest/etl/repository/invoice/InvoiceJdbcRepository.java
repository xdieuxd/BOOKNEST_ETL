package com.booknest.etl.repository.invoice;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.booknest.etl.dto.InvoiceRawMessage;

@Repository
public class InvoiceJdbcRepository {

    private final JdbcTemplate jdbcTemplate;

    public InvoiceJdbcRepository(JdbcTemplate sourceJdbcTemplate) {
        this.jdbcTemplate = sourceJdbcTemplate;
    }

    public List<InvoiceRawMessage> findAll() {
        String sql = """
                SELECT * FROM hoa_don
                """;
        return jdbcTemplate.query(sql, this::mapRow);
    }

    public Optional<InvoiceRawMessage> findById(String id) {
        return jdbcTemplate.query("SELECT * FROM hoa_don WHERE ma_hoa_don = ?", this::mapRow, id)
                .stream().findFirst();
    }

    private InvoiceRawMessage mapRow(ResultSet rs, int rowNum) throws SQLException {
        return InvoiceRawMessage.builder()
                .invoiceId(String.valueOf(rs.getInt("ma_hoa_don")))
                .orderId(String.valueOf(rs.getInt("ma_don_hang")))
                .amount(rs.getBigDecimal("so_tien"))
                .status(rs.getString("trang_thai_thanh_toan"))
                .createdAt(rs.getTimestamp("ngay_tao") != null
                        ? rs.getTimestamp("ngay_tao").toInstant().atOffset(OffsetDateTime.now().getOffset())
                        : null)
                .source("source_db")
                .extractedAt(OffsetDateTime.now())
                .build();
    }
}
