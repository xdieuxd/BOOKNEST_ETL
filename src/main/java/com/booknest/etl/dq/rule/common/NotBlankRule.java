package com.booknest.etl.dq.rule.common;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.StringUtils;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NotBlankRule<T> implements DataQualityRule<T> {

    private final String field;
    private final String message;
    private final Function<T, String> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        String value = extractor.apply(payload);
        if (!StringUtils.hasText(value)) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("NOT_BLANK")
                    .message(message)
                    .build());
        }
    }
}
