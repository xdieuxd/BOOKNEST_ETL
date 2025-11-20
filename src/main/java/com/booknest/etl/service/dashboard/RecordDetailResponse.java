package com.booknest.etl.service.dashboard;

import java.util.Map;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RecordDetailResponse {
    String entityType;
    String entityKey;
    Map<String, Object> rawData;
    Map<String, Object> stagingData;
    Map<String, Object> dqResult;
}
