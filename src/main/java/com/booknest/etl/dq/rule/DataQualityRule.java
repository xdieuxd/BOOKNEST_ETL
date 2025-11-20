package com.booknest.etl.dq.rule;

import java.util.List;

import com.booknest.etl.dto.DqErrorDto;

public interface DataQualityRule<T> {
    void validate(T payload, List<DqErrorDto> errors);
}
