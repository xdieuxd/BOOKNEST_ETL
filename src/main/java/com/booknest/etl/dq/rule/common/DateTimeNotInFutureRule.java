package com.booknest.etl.dq.rule.common;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Function;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DateTimeNotInFutureRule<T> implements DataQualityRule<T> {

    private final String field;
    private final String message;
    private final Function<T, OffsetDateTime> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        OffsetDateTime value = extractor.apply(payload);
        if (value == null) {
            return;
        }
        if (value.isAfter(OffsetDateTime.now())) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("DATETIME_NOT_FUTURE")
                    .message(message)
                    .build());
        }
    }
}
