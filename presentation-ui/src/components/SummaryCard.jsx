import React from "react";
import "./SummaryCard.css";

export default function SummaryCard({ summary }) {
  return (
    <div className="summary-card card">
      <div className="summary-header">
        <h2>Bảng điều khiển ETL</h2>
        <p className="muted">
          Tổng quan trạng thái pipeline và kết quả kiểm tra dữ liệu
        </p>
      </div>

      <div className="summary-grid">
        <div className="stat">
          <div className="stat-label">Tổng bản ghi đã kiểm tra</div>
          <div className="stat-value">{summary?.totalProcessed ?? 0}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Tổng bản ghi staging</div>
          <div className="stat-value">{summary?.totalStaging ?? 0}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Đã qua</div>
          <div className="stat-value text-success">{summary?.passed ?? 0}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Đã sửa</div>
          <div className="stat-value text-warning">{summary?.fixed ?? 0}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Bị lỗi</div>
          <div className="stat-value text-danger">{summary?.failed ?? 0}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Lần chạy gần nhất</div>
          <div className="stat-value">
            {summary?.lastRun ?? "Chưa có dữ liệu"}
          </div>
        </div>
      </div>
    </div>
  );
}
