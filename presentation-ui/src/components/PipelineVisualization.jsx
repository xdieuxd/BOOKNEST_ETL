import "./PipelineVisualization.css";

export default function PipelineVisualization({ state }) {
  const stages = [
    {
      name: "Extract",
      icon: "📥",
      color: "#3b82f6",
      count: state?.extract?.totalRecords || 0,
    },
    {
      name: "Data Quality",
      icon: "✓",
      color: "#10b981",
      count: state?.dq?.passed || 0,
      failed: state?.dq?.failed || 0,
    },
    {
      name: "Transform",
      icon: "🔄",
      color: "#f59e0b",
      count: state?.transform?.processed || 0,
    },
    {
      name: "Load",
      icon: "💾",
      color: "#8b5cf6",
      count: state?.load?.loaded || 0,
    },
  ];

  return (
    <div className="pipeline-viz">
      <h2 className="pipeline-title">Luồng Xử Lý Pipeline</h2>
      <div className="pipeline-flow">
        {stages.map((stage, idx) => (
          <div key={stage.name}>
            <div className="stage-box" style={{ borderColor: stage.color }}>
              <div className="stage-icon">{stage.icon}</div>
              <div className="stage-name">{stage.name}</div>
              <div className="stage-count">{stage.count} record</div>
              {stage.failed && (
                <div className="stage-failed">{stage.failed} lỗi</div>
              )}
            </div>
            {idx < stages.length - 1 && <div className="stage-arrow">→</div>}
          </div>
        ))}
      </div>

      <div className="pipeline-stats">
        <div className="stat-card success">
          <div className="stat-label">Đã Qua DQ</div>
          <div className="stat-value">{state?.dq?.passed || 0}</div>
        </div>
        <div className="stat-card danger">
          <div className="stat-label">Bị Lỗi</div>
          <div className="stat-value">{state?.dq?.failed || 0}</div>
        </div>
        <div className="stat-card warning">
          <div className="stat-label">Sẽ Sửa</div>
          <div className="stat-value">{state?.dq?.fixable || 0}</div>
        </div>
        <div className="stat-card info">
          <div className="stat-label">Đã Tải</div>
          <div className="stat-value">{state?.load?.loaded || 0}</div>
        </div>
      </div>
    </div>
  );
}
