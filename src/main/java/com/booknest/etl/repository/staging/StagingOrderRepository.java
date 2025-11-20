package com.booknest.etl.repository.staging;

import java.sql.Types;
import java.time.OffsetDateTime;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dq.DataQualityStatus;

@Repository
public class StagingOrderRepository {

    private final JdbcTemplate stagingJdbcTemplate;

    public StagingOrderRepository(JdbcTemplate stagingJdbcTemplate) {
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }

    public void upsert(OrderRawMessage order, DataQualityStatus qualityStatus, String errors) {
        String sql = """
                INSERT INTO stg_orders (order_key, customer_key, status, payment_method,
                                        subtotal, discount, shipping_fee, total_amount,
                                        payment_ref, receiver_name, receiver_phone, receiver_address,
                                        order_date, updated_at, quality_status, quality_errors, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                ON DUPLICATE KEY UPDATE
                    customer_key = VALUES(customer_key),
                    status = VALUES(status),
                    payment_method = VALUES(payment_method),
                    subtotal = VALUES(subtotal),
                    discount = VALUES(discount),
                    shipping_fee = VALUES(shipping_fee),
                    total_amount = VALUES(total_amount),
                    payment_ref = VALUES(payment_ref),
                    receiver_name = VALUES(receiver_name),
                    receiver_phone = VALUES(receiver_phone),
                    receiver_address = VALUES(receiver_address),
                    order_date = VALUES(order_date),
                    updated_at = VALUES(updated_at),
                    quality_status = VALUES(quality_status),
                    quality_errors = VALUES(quality_errors),
                    loaded_at = NOW()
                """;

        stagingJdbcTemplate.update(sql, new Object[]{
                order.getOrderId(),
                order.getCustomerEmail(),
                order.getStatus(),
                order.getPaymentMethod(),
                orderSubtotal(order),
                order.getDiscount(),
                order.getShippingFee(),
                order.getTotalAmount(),
                null,
                order.getCustomerName(),
                null,
                null,
                order.getCreatedAt(),
                order.getExtractedAt(),
                qualityStatus != null ? qualityStatus.value() : null,
                errors
        }, new int[]{
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.DECIMAL, Types.DECIMAL, Types.DECIMAL, Types.DECIMAL,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.LONGVARCHAR
        });
    }

    private java.math.BigDecimal orderSubtotal(OrderRawMessage order) {
        if (order.getItems() == null) {
            return null;
        }
        return order.getItems().stream()
                .filter(item -> item.getUnitPrice() != null && item.getQuantity() != null)
                .map(item -> item.getUnitPrice().multiply(java.math.BigDecimal.valueOf(item.getQuantity())))
                .reduce(java.math.BigDecimal.ZERO, java.math.BigDecimal::add);
    }
}
