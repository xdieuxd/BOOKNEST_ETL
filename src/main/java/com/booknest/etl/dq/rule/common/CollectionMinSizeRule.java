package com.booknest.etl.dq.rule.common;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.DqErrorDto;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CollectionMinSizeRule<T> implements DataQualityRule<T> {

    private final String field;
    private final int minSize;
    private final String message;
    private final Function<T, Collection<?>> extractor;

    @Override
    public void validate(T payload, List<DqErrorDto> errors) {
        Collection<?> collection = extractor.apply(payload);
        if (collection == null || collection.size() < minSize) {
            errors.add(DqErrorDto.builder()
                    .field(field)
                    .rule("MIN_SIZE")
                    .message(message)
                    .build());
        }
    }
}
