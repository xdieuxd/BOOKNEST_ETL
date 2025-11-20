package com.booknest.etl.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class OrderRawMessage {
    String source;
    String orderId;
    String customerName;
    String customerEmail;
    String status;
    String paymentMethod;
    BigDecimal totalAmount;
    BigDecimal discount;
    BigDecimal shippingFee;
    List<OrderItemRawMessage> items;
    OffsetDateTime createdAt;
    OffsetDateTime extractedAt;
}
