import React, { useState } from "react";
import SummaryCard from "./SummaryCard";
import DetailModal from "./DetailModal";
import PipelineVisualization from "./PipelineVisualization";
import EntityTabs from "./EntityTabs";
import "./ResultsView.css";

export default function ResultsView({ pipelineState, onRefresh }) {
  const [selectedRecord, setSelectedRecord] = useState(null);
  const [activeEntity, setActiveEntity] = useState("books");

  const byEntity = pipelineState?.results?.byEntity;
  const dqByEntity = pipelineState?.results?.dqByEntity || {};

  const countMapItems = (map) =>
    Object.values(map || {}).reduce(
      (sum, arr) => sum + (Array.isArray(arr) ? arr.length : 0),
      0
    );

  const summary = {
    totalProcessed:
      pipelineState?.extract?.totalRecords ?? countMapItems(byEntity?.raw) ?? 0,
    totalStaging:
      countMapItems(byEntity?.transformed) + countMapItems(byEntity?.errors),
    passed:
      pipelineState?.dq?.passed ?? countMapItems(byEntity?.transformed) ?? 0,
    fixed: pipelineState?.dq?.fixable ?? 0,
    failed: pipelineState?.dq?.failed ?? countMapItems(byEntity?.errors) ?? 0,
    lastRun: pipelineState?.tracingId ?? null,
  };

  const handleEntityChange = (entity) => {
    setActiveEntity(entity);
  };

  return (
    <div className="results-view">
      <SummaryCard summary={summary} />

      <div className="top-area">
        <PipelineVisualization state={pipelineState} />
      </div>

      {/* Refresh Button - Prominent placement */}
      <div
        style={{
          padding: "20px",
          textAlign: "center",
          background: "#f0f8ff",
          borderRadius: "8px",
          margin: "10px 0",
        }}
      >
        <button
          onClick={onRefresh}
          style={{
            padding: "12px 24px",
            fontSize: "16px",
            fontWeight: "bold",
            background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
            color: "white",
            border: "none",
            borderRadius: "8px",
            cursor: "pointer",
            boxShadow: "0 4px 6px rgba(0,0,0,0.1)",
            transition: "all 0.3s",
          }}
          onMouseOver={(e) => (e.target.style.transform = "scale(1.05)")}
          onMouseOut={(e) => (e.target.style.transform = "scale(1)")}
        >
          Làm mới kết quả
        </button>
      </div>

      {byEntity ? (
        <EntityTabs
          byEntity={byEntity}
          dqByEntity={dqByEntity}
          onSelect={setSelectedRecord}
          onEntityChange={handleEntityChange}
        />
      ) : (
        <div className="no-data">
          <p>Đang tải dữ liệu...</p>
        </div>
      )}

      <DetailModal
        record={selectedRecord}
        onClose={() => setSelectedRecord(null)}
      />
    </div>
  );
}
