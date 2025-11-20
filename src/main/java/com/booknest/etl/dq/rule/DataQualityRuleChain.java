package com.booknest.etl.dq.rule;

import java.util.ArrayList;
import java.util.List;

import com.booknest.etl.dto.DqErrorDto;

public class DataQualityRuleChain<T> {

    private final List<DataQualityRule<T>> rules = new ArrayList<>();

    public DataQualityRuleChain<T> addRule(DataQualityRule<T> rule) {
        rules.add(rule);
        return this;
    }

    public List<DqErrorDto> validate(T payload) {
        List<DqErrorDto> errors = new ArrayList<>();
        for (DataQualityRule<T> rule : rules) {
            rule.validate(payload, errors);
        }
        return errors;
    }
}
