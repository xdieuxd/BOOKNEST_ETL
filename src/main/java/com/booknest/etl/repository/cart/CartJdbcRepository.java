package com.booknest.etl.repository.cart;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.booknest.etl.dto.CartItemRawMessage;
import com.booknest.etl.dto.CartRawMessage;

@Repository
public class CartJdbcRepository {

    private final JdbcTemplate jdbcTemplate;

    public CartJdbcRepository(JdbcTemplate sourceJdbcTemplate) {
        this.jdbcTemplate = sourceJdbcTemplate;
    }

    public List<CartRawMessage> findAll() {
        String sql = """
                SELECT gh.ma_gio_hang,
                       gh.ma_nguoi_dung,
                       gh.ngay_tao,
                       cth.ma_sach,
                       cth.so_luong,
                       cth.gia_ban
                FROM gio_hang gh
                LEFT JOIN chi_tiet_gio_hang cth ON cth.ma_gio_hang = gh.ma_gio_hang
                """;
        Map<Integer, CartRawMessage.CartRawMessageBuilder> carts = new HashMap<>();
        Map<Integer, List<CartItemRawMessage>> cartItems = new HashMap<>();
        jdbcTemplate.query(sql, rs -> {
            while (rs.next()) {
                processRow(rs, carts, cartItems);
            }
            return null;
        });
        List<CartRawMessage> result = new ArrayList<>();
        carts.forEach((id, builder) -> result.add(builder
                .items(cartItems.getOrDefault(id, List.of()))
                .build()));
        return result;
    }

    public Optional<CartRawMessage> findById(String id) {
        return findAll().stream()
                .filter(cart -> cart.getCartId().equals(id))
                .findFirst();
    }

    private void processRow(ResultSet rs,
                            Map<Integer, CartRawMessage.CartRawMessageBuilder> carts,
                            Map<Integer, List<CartItemRawMessage>> cartItems) {
        try {
            int cartId = rs.getInt("ma_gio_hang");
            String customerId = String.valueOf(rs.getInt("ma_nguoi_dung"));
            OffsetDateTime createdAt = rs.getTimestamp("ngay_tao") != null
                    ? rs.getTimestamp("ngay_tao").toInstant().atOffset(OffsetDateTime.now().getOffset())
                    : null;

            carts.computeIfAbsent(cartId, id -> CartRawMessage.builder()
                    .cartId(String.valueOf(id))
                    .customerId(customerId)
                    .createdAt(createdAt)
                    .source("source_db")
                    .extractedAt(OffsetDateTime.now()));

            if (rs.getObject("ma_sach") != null) {
                CartItemRawMessage item = CartItemRawMessage.builder()
                        .bookId(String.valueOf(rs.getInt("ma_sach")))
                        .quantity(rs.getInt("so_luong"))
                        .unitPrice(rs.getBigDecimal("gia_ban"))
                        .build();
                cartItems.computeIfAbsent(cartId, key -> new ArrayList<>()).add(item);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot map cart row", e);
        }
    }
}
