package com.booknest.etl.service.tracking;

import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Service để theo dõi message qua các stage của pipeline
 * Mỗi record sẽ được gán một tracing ID duy nhất
 */
@Service
public class MessageTracingService {

    /**
     * Tạo tracing ID duy nhất cho một record
     */
    public String generateTracingId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Tạo tracing ID từ entity key (để đảm bảo cùng record có cùng ID)
     */
    public String generateTracingId(String entityType, String entityKey) {
        String combined = entityType + ":" + entityKey;
        // Tạo UUID từ combined string (deterministic)
        return UUID.nameUUIDFromBytes(combined.getBytes()).toString();
    }

    /**
     * Format tracing context cho logging
     */
    public String formatTracingContext(String tracingId, String stage, String status) {
        return String.format("[TRACE:%s] Stage:%s Status:%s", tracingId.substring(0, 8), stage, status);
    }
}
