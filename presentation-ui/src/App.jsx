import { useState } from "react";
import axios from "axios";
import UploadSection from "./components/UploadSection";
import ResultsView from "./components/ResultsView";
import "./App.css";

export default function App() {
  const [pipelineState, setPipelineState] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleUpload = async (file) => {
    setLoading(true);
    setError(null);
    setPipelineState(null);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const response = await axios.post("/api/etl/upload", formData, {
        headers: { "Content-Type": "multipart/form-data" },
      });

      setPipelineState(response.data);
    } catch (err) {
      setError(err.response?.data?.message || "Lỗi khi tải file");
    } finally {
      setLoading(false);
    }
  };

  const handleReprocess = async (correctedData) => {
    setLoading(true);
    setError(null);

    try {
      const response = await axios.post("/api/etl/reprocess", correctedData);

      const updatedState = {
        ...pipelineState,
        results: response.data.results,
        dq: {
          passed: response.data.results.transformed.length,
          failed: response.data.results.errors.length,
          fixable: response.data.results.errors.filter((e) =>
            e._errors?.some((err) => err.rule === "NOT_BLANK")
          ).length,
        },
        tracingId: response.data.tracingId,
      };
      setPipelineState(updatedState);
    } catch (err) {
      console.error("Lỗi reprocess:", err);
      setError(err.response?.data?.message || "Lỗi khi xử lý lại");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app-container">
      <header className="app-header">
        <div className="header-content">
          <h1>📊 ETL Pipeline Booknest</h1>
        </div>
      </header>

      <main className="app-main">
        <div className="upload-area">
          <UploadSection onUpload={handleUpload} loading={loading} />
        </div>

        {error && (
          <div className="error-banner">
            <span>⚠️ {error}</span>
          </div>
        )}

        {pipelineState && (
          <ResultsView
            pipelineState={pipelineState}
            onReprocess={handleReprocess}
          />
        )}

        {loading && (
          <div className="loading-overlay">
            <div className="spinner"></div>
            <p>Đang xử lý...</p>
          </div>
        )}
      </main>

      <footer className="app-footer">
        <p>© 2024 Booknest ETL. Giao diện trình bày kết quả.</p>
      </footer>
    </div>
  );
}
