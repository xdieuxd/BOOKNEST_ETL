package com.booknest.etl.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class InvoiceRawMessage {
    String invoiceId;
    String orderId;
    BigDecimal amount;
    String status;
    OffsetDateTime createdAt;
    String source;
    OffsetDateTime extractedAt;
}
