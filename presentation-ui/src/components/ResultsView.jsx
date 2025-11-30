import React, { useState } from "react";
import SummaryCard from "./SummaryCard";
import RecordTable from "./RecordTable";
import DetailModal from "./DetailModal";
import ResultsPanel from "./ResultsPanel";
import PipelineVisualization from "./PipelineVisualization";
import "./ResultsView.css";

export default function ResultsView({ pipelineState, onReprocess }) {
  const [selectedRecord, setSelectedRecord] = useState(null);

  const summary = {
    totalProcessed: pipelineState?.extract?.totalRecords ?? 0,
    totalStaging: pipelineState?.transform?.processed ?? 0,
    passed: pipelineState?.dq?.passed ?? 0,
    fixed: pipelineState?.dq?.fixable ?? 0,
    failed: pipelineState?.dq?.failed ?? 0,
    lastRun: pipelineState?.tracingId ?? null,
  };

  const transformed = pipelineState?.results?.transformed || [];
  const errors = pipelineState?.results?.errors || [];

  return (
    <div className="results-view">
      <SummaryCard summary={summary} />

      <div className="top-area">
        <PipelineVisualization state={pipelineState} />
      </div>

      <div className="tables-grid">
        <div className="left">
          <h3>Dữ liệu đã Transform (PASS/FIXED)</h3>
          <RecordTable
            title="Dữ liệu đã Transform"
            data={transformed}
            color="success"
            onSelect={setSelectedRecord}
          />
        </div>
        <div className="right">
          <h3>Lỗi Data Quality</h3>
          <RecordTable
            title="Lỗi Data Quality"
            data={errors}
            color="danger"
            onSelect={setSelectedRecord}
          />
        </div>
      </div>

      <div className="results-panel-area">
        <ResultsPanel state={pipelineState} onReprocess={onReprocess} />
      </div>

      <DetailModal
        record={selectedRecord}
        onClose={() => setSelectedRecord(null)}
      />
    </div>
  );
}
