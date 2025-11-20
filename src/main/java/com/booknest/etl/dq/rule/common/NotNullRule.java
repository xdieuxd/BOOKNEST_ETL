package com.booknest.etl.dq.rule.common;

import java.util.List;
import java.util.function.Function;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NotNullRule<T, R> implements DataQualityRule<T> {

    private final String field;
    private final String message;
    private final Function<T, R> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        if (extractor.apply(payload) == null) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("NOT_NULL")
                    .message(message)
                    .build());
        }
    }
}
