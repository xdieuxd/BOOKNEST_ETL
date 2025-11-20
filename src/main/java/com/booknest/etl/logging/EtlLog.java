package com.booknest.etl.logging;

import java.time.OffsetDateTime;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class EtlLog {
    String jobName;
    String stage;
    String status;
    String message;
    String sourceRecord;
    String targetRecord;
    OffsetDateTime startedAt;
    OffsetDateTime finishedAt;
}
