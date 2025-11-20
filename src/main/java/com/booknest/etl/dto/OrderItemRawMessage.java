package com.booknest.etl.dto;

import java.math.BigDecimal;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class OrderItemRawMessage {
    String bookId;
    Integer quantity;
    BigDecimal unitPrice;
}
