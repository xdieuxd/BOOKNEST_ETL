package com.booknest.etl.service.dashboard;

import java.time.OffsetDateTime;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DashboardSummary {
    long totalProcessed;
    long totalStaging;
    long passed;
    long fixed;
    long failed;
    OffsetDateTime lastRun;
}
