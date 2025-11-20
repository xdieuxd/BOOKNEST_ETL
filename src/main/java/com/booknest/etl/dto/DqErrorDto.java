package com.booknest.etl.dto;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DqErrorDto {
    String field;
    String rule;
    String message;
}
