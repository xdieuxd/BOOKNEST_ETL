package com.booknest.etl.web;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.booknest.etl.service.dashboard.DashboardService;
import com.booknest.etl.service.dashboard.DashboardSummary;
import com.booknest.etl.service.dashboard.RecordDetailResponse;
import com.booknest.etl.service.dashboard.RecordDetailService;
import com.booknest.etl.service.dashboard.TransformedDataService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/etl/dashboard")
@RequiredArgsConstructor
public class DashboardController {

    private final DashboardService dashboardService;
    private final TransformedDataService transformedDataService;
    private final RecordDetailService recordDetailService;

    @GetMapping
    public DashboardSummary summary() {
        return dashboardService.getSummary();
    }

    @GetMapping("/transformed")
    public List<Map<String, Object>> transformed(@RequestParam(defaultValue = "BOOK") String entity) {
        return transformedDataService.getTransformed(entity);
    }

    @GetMapping("/errors")
    public List<Map<String, Object>> errors() {
        return transformedDataService.getErrors();
    }

    @GetMapping("/records/{entityType}/{entityKey}")
    public ResponseEntity<RecordDetailResponse> detail(@PathVariable String entityType,
                                                       @PathVariable String entityKey) {
        return ResponseEntity.ok(recordDetailService.getRecordDetail(entityType, entityKey));
    }

    @GetMapping("/staging-results")
    public ResponseEntity<Map<String, Object>> getStagingResults() {
        DashboardSummary summary = dashboardService.getSummary();
        Map<String, Object> rawByEntity = transformedDataService.getRawByEntity();
        Map<String, Object> transformedByEntity = transformedDataService.getTransformedByEntity();
        Map<String, Object> errorsByEntity = transformedDataService.getErrorsByEntity();
        
        // Build dqByEntity - count passed/failed per entity
        Map<String, Map<String, Integer>> dqByEntity = new java.util.HashMap<>();
        for (String entity : rawByEntity.keySet()) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> transformed = (List<Map<String, Object>>) transformedByEntity.get(entity);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> errors = (List<Map<String, Object>>) errorsByEntity.get(entity);
            
            dqByEntity.put(entity, Map.of(
                "passed", transformed != null ? transformed.size() : 0,
                "failed", errors != null ? errors.size() : 0,
                "fixable", 0
            ));
        }
        
        Map<String, Object> response = new java.util.HashMap<>();
        response.put("extract", Map.of(
            "totalRecords", summary.getTotalProcessed(),
            "status", "COMPLETED"
        ));
        response.put("transform", Map.of(
            "processed", summary.getPassed()
        ));
        response.put("load", Map.of(
            "loaded", summary.getPassed()
        ));
        response.put("dq", Map.of(
            "passed", summary.getPassed(),
            "failed", summary.getFailed(),
            "fixable", summary.getFixed()
        ));
        response.put("results", Map.of(
            "byEntity", Map.of(
                "raw", rawByEntity,
                "transformed", transformedByEntity,
                "errors", errorsByEntity
            ),
            "dqByEntity", dqByEntity
        ));
        response.put("tracingId", summary.getLastRun() != null ? summary.getLastRun().toString() : "N/A");
        
        return ResponseEntity.ok(response);
    }
}
