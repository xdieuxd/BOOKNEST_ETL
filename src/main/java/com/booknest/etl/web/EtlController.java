package com.booknest.etl.web;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.booknest.etl.service.orchestrator.EtlOrchestratorService;
import com.booknest.etl.service.staging.StagingSummaryService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/etl")
@RequiredArgsConstructor
public class EtlController {

    private final EtlOrchestratorService orchestratorService;
    private final StagingSummaryService stagingSummaryService;

    @PostMapping("/run/database")
    public ResponseEntity<String> triggerDatabaseExtract() {
        orchestratorService.runDatabaseExtract();
        return ResponseEntity.ok("Database extract triggered");
    }

    @PostMapping("/run/csv")
    public ResponseEntity<String> triggerCsvExtract() {
        orchestratorService.runCsvExtract();
        return ResponseEntity.ok("CSV extract triggered");
    }

    @PostMapping("/run/all")
    public ResponseEntity<String> triggerFullPipeline() {
        orchestratorService.runDatabaseExtract();
        orchestratorService.runCsvExtract();
        return ResponseEntity.ok("Full pipeline extract triggered");
    }

    @GetMapping("/summary")
    public ResponseEntity<Map<String, Long>> getSummary() {
        return ResponseEntity.ok(stagingSummaryService.loadSummary());
    }
}
