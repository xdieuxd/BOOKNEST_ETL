package com.booknest.etl.dq.rule.common;

import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RegexRule<T> implements DataQualityRule<T> {

    private final String field;
    private final String message;
    private final Pattern pattern;
    private final Function<T, String> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        String value = extractor.apply(payload);
        if (value == null || value.isBlank()) {
            return;
        }
        if (!pattern.matcher(value).matches()) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("REGEX")
                    .message(message)
                    .build());
        }
    }
}
