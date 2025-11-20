package com.booknest.etl.dto;

import java.time.OffsetDateTime;
import java.util.List;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class EtlResultDto {
    String entityType;
    String entityKey;
    boolean success;
    List<DqErrorDto> errors;
    String message;
    OffsetDateTime processedAt;
}
