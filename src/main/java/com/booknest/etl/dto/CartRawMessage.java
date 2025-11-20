package com.booknest.etl.dto;

import java.time.OffsetDateTime;
import java.util.List;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class CartRawMessage {
    String cartId;
    String customerId;
    OffsetDateTime createdAt;
    List<CartItemRawMessage> items;
    String source;
    OffsetDateTime extractedAt;
}
