package com.booknest.etl.service.tracking;

import java.util.UUID;
import org.springframework.stereotype.Service;


@Service
public class MessageTracingService {

    public String generateTracingId() {
        return UUID.randomUUID().toString();
    }

    public String generateTracingId(String entityType, String entityKey) {
        String combined = entityType + ":" + entityKey;
        return UUID.nameUUIDFromBytes(combined.getBytes()).toString();
    }

    public String formatTracingContext(String tracingId, String stage, String status) {
        return String.format("[TRACE:%s] Stage:%s Status:%s", tracingId.substring(0, 8), stage, status);
    }
}
