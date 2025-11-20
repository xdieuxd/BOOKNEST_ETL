package com.booknest.etl.dq.rule.common;

import java.util.List;
import java.util.function.Function;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PositiveIntegerRule<T> implements DataQualityRule<T> {

    private final String field;
    private final boolean allowZero;
    private final String message;
    private final Function<T, Integer> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        Integer value = extractor.apply(payload);
        if (value == null) {
            return;
        }
        boolean valid = allowZero ? value >= 0 : value > 0;
        if (!valid) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("POSITIVE_INT")
                    .message(message)
                    .build());
        }
    }
}
