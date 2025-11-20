package com.booknest.etl.dq.rule.common;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PositiveDecimalRule<T> implements DataQualityRule<T> {

    private final String field;
    private final boolean allowZero;
    private final String message;
    private final Function<T, BigDecimal> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        BigDecimal value = extractor.apply(payload);
        if (value == null) {
            return;
        }
        int compare = value.compareTo(BigDecimal.ZERO);
        boolean valid = allowZero ? compare >= 0 : compare > 0;
        if (!valid) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("POSITIVE")
                    .message(message)
                    .build());
        }
    }
}
