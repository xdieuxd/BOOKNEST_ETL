package com.booknest.etl.dq.rule.common;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StringSetRule<T> implements DataQualityRule<T> {

    private final String field;
    private final Set<String> allowedValues;
    private final String message;
    private final Function<T, String> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        String value = extractor.apply(payload);
        if (value == null) {
            return;
        }
        if (!allowedValues.contains(value)) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("ALLOWED_SET")
                    .message(message)
                    .build());
        }
    }
}
