import { useState, useEffect } from "react";
import axios from "axios";
import ResultsView from "./components/ResultsView";
import "./App.css";

export default function App() {
  const [pipelineState, setPipelineState] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const countMapItems = (map) =>
    Object.values(map || {}).reduce(
      (sum, arr) => sum + (Array.isArray(arr) ? arr.length : 0),
      0
    );

  const hasData = (data) => {
    const byEntity = data?.results?.byEntity || {};
    return (
      countMapItems(byEntity.raw) > 0 ||
      countMapItems(byEntity.transformed) > 0 ||
      countMapItems(byEntity.errors) > 0
    );
  };

  // Load dữ liệu CÓ SẴN từ staging_db (nhanh)
  const loadExistingResults = async () => {
    setLoading(true);
    setError(null);

    try {
      console.log("📊 Loading existing results from staging_db...");
      const response = await axios.get("/api/etl/dashboard/staging-results");
      console.log("✅ Results loaded:", response.data);

      if (hasData(response.data)) {
        setPipelineState(response.data);
      } else {
        setPipelineState(response.data);
      }
    } catch (err) {
      console.error("❌ Load error:", err);
      setError(
        err.response?.data?.message || err.message || "Lỗi khi tải dữ liệu"
      );
    } finally {
      setLoading(false);
    }
  };

  // Chạy FULL pipeline (chậm: 5-10s)
  const runFullPipeline = async () => {
    setLoading(true);
    setError(null);

    try {
      console.log("🚀 Starting full ETL pipeline...");
      // Chạy full ETL pipeline: Extract → RabbitMQ → Consumers → staging_db
      const response = await axios.post(
        "/api/etl/batch-extract-with-results",
        {},
        { timeout: 120000 }
      );
      console.log("✅ Pipeline executed successfully:", response.data);

      if (hasData(response.data)) {
        setPipelineState(response.data);
      } else if (!pipelineState) {
        setPipelineState(response.data);
      }
    } catch (err) {
      console.error("❌ Pipeline error:", err);
      console.error("Error details:", {
        status: err.response?.status,
        message: err.response?.data?.message,
        timeout: err.code === "ECONNABORTED",
      });

      if (err.code === "ECONNABORTED") {
        setError(
          "Pipeline đang chạy quá lâu. Vui lòng đợi và refresh lại sau."
        );
      } else {
        setError(
          err.response?.data?.message ||
            err.message ||
            "Lỗi khi chạy ETL pipeline"
        );
      }
    } finally {
      setLoading(false);
    }
  };

  // Load dữ liệu có sẵn khi mở page (NHANH)
  useEffect(() => {
    loadExistingResults();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="app-container">
      <header className="app-header">
        <div className="header-content">
          <h1>ETL Booknest - Demo</h1>
          <div style={{ marginLeft: "20px", display: "flex", gap: "10px" }}>
            <button
              onClick={loadExistingResults}
              disabled={loading}
              style={{
                padding: "10px 20px",
                fontSize: "14px",
                cursor: loading ? "not-allowed" : "pointer",
                opacity: loading ? 0.6 : 1,
                background: "#4CAF50",
                color: "white",
                border: "none",
                borderRadius: "4px",
              }}
            >
              📊 Tải dữ liệu hiện tại
            </button>
            <button
              onClick={runFullPipeline}
              disabled={loading}
              style={{
                padding: "10px 20px",
                fontSize: "14px",
                cursor: loading ? "not-allowed" : "pointer",
                opacity: loading ? 0.6 : 1,
                background: "#764ba2",
                color: "white",
                border: "none",
                borderRadius: "4px",
              }}
            >
              🚀 Chạy lại ETL Pipeline
            </button>
          </div>
        </div>
      </header>

      <main className="app-main">
        {error && (
          <div className="error-banner">
            <span>{error}</span>
          </div>
        )}

        {pipelineState && (
          <ResultsView
            pipelineState={pipelineState}
            onRefresh={loadExistingResults}
          />
        )}

        {loading && (
          <div className="loading-overlay">
            <div className="spinner"></div>
            <p>Dang xu ly...</p>
          </div>
        )}
      </main>

      <footer className="app-footer">
        <p>(c) 2024 Booknest ETL. Giao dien trinh bay ket qua.</p>
      </footer>
    </div>
  );
}
