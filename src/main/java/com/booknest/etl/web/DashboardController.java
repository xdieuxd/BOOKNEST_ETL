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
}
