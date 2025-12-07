import React, { useState } from "react";
import RecordTable from "./RecordTable";
import "./EntityTabs.css";

export default function EntityTabs({
  byEntity,
  dqByEntity,
  onSelect,
  onEntityChange,
}) {
  const entityTypes = Object.keys(byEntity.raw || {});
  const [activeEntity, setActiveEntity] = useState(entityTypes[0] || "books");
  const [activeTab, setActiveTab] = useState("raw"); // Default to raw since data is sent to RabbitMQ first

  const handleEntityChange = (entity) => {
    setActiveEntity(entity);
    if (onEntityChange) {
      onEntityChange(entity);
    }
  };

  if (entityTypes.length === 0) {
    console.log("EntityTabs - NO ENTITY TYPES!");
    return <div className="no-data">Không có dữ liệu</div>;
  }

  const currentRaw = byEntity.raw[activeEntity] || [];
  const currentTransformed = byEntity.transformed[activeEntity] || [];
  const currentErrors = byEntity.errors[activeEntity] || [];
  const currentDq = dqByEntity[activeEntity] || {
    passed: 0,
    failed: 0,
    fixable: 0,
  };

  const entityLabels = {
    books: "SACH",
    customers: "KHACH HANG",
    orders: "DON HANG",
    order_items: "CHI TIET DON",
    carts: "GIO HANG",
    invoices: "HOA DON",
  };

  return (
    <div className="entity-tabs-container">
      {/* Entity Type Selector */}
      <div className="entity-selector">
        <div className="entity-buttons">
          {entityTypes.map((entity) => {
            const dq = dqByEntity[entity] || {};
            return (
              <button
                key={entity}
                className={`entity-btn ${
                  activeEntity === entity ? "active" : ""
                }`}
                onClick={() => handleEntityChange(entity)}
              >
                <span className="entity-label">
                  {entityLabels[entity] || entity}
                </span>
                <span className="entity-badge">
                  {byEntity.raw[entity]?.length || 0} records
                </span>
                <div className="entity-stats">
                  <span className="stat-pass">OK {dq.passed || 0}</span>
                  <span className="stat-fail">LỖI {dq.failed || 0}</span>
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* View Tabs (Raw/Transformed/Errors) */}
      <div className="view-tabs">
        <button
          className={`tab-btn ${activeTab === "raw" ? "active" : ""}`}
          onClick={() => setActiveTab("raw")}
        >
          Raw ({currentRaw.length})
        </button>
        <button
          className={`tab-btn ${activeTab === "transformed" ? "active" : ""}`}
          onClick={() => setActiveTab("transformed")}
        >
          Transformed ({currentTransformed.length})
        </button>
        <button
          className={`tab-btn ${activeTab === "errors" ? "active" : ""}`}
          onClick={() => setActiveTab("errors")}
        >
          Errors ({currentErrors.length})
        </button>
      </div>

      {/* Data Quality Summary */}
      <div className="dq-summary">
        <div className="dq-item">
          <span className="dq-label">Passed:</span>
          <span className="dq-value success">{currentDq.passed}</span>
        </div>
        <div className="dq-item">
          <span className="dq-label">Failed:</span>
          <span className="dq-value danger">{currentDq.failed}</span>
        </div>
        <div className="dq-item">
          <span className="dq-label">Fixable:</span>
          <span className="dq-value warning">{currentDq.fixable}</span>
        </div>
      </div>

      {/* Table Content */}
      <div className="table-content">
        {activeTab === "raw" && (
          <RecordTable
            title={`Raw Data - ${entityLabels[activeEntity]}`}
            data={currentRaw}
            color="info"
            onSelect={onSelect}
          />
        )}
        {activeTab === "transformed" && (
          <RecordTable
            title={`Transformed Data - ${entityLabels[activeEntity]}`}
            data={currentTransformed}
            color="success"
            onSelect={onSelect}
          />
        )}
        {activeTab === "errors" && (
          <RecordTable
            title={`Validation Errors - ${entityLabels[activeEntity]}`}
            data={currentErrors}
            color="danger"
            onSelect={onSelect}
          />
        )}
      </div>
    </div>
  );
}
