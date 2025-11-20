package com.booknest.etl.dq;

public enum DataQualityStatus {
    RAW,
    PASSED,
    FAILED,
    VALIDATED,
    REJECTED,
    FIXED;

    public String value() {
        return name();
    }
}
