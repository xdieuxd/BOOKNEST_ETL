package com.booknest.etl.dq.rule.common;

import java.util.List;
import java.util.function.Function;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MaxLengthRule<T> implements DataQualityRule<T> {

    private final String field;
    private final int maxLength;
    private final String message;
    private final Function<T, String> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        String value = extractor.apply(payload);
        if (value != null && value.length() > maxLength) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("MAX_LENGTH")
                    .message(message)
                    .build());
        }
    }
}
