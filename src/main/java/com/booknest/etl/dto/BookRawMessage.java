package com.booknest.etl.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BookRawMessage {
    String source; // db or csv
    String bookId;
    String title;
    String description;
    BigDecimal price;
    boolean free;
    LocalDate releasedAt;
    String status;
    BigDecimal averageRating;
    Integer totalOrders;
    List<String> authors;
    List<String> categories;
    OffsetDateTime extractedAt;
}
