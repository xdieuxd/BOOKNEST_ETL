import { useRef } from "react";
import "./UploadSection.css";

export default function UploadSection({ onUpload, loading }) {
  const fileInputRef = useRef(null);

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    const files = e.dataTransfer.files;
    if (files.length > 0) {
      const file = files[0];
      if (file.name.endsWith(".csv")) {
        onUpload(file);
      } else {
        alert("Vui lòng chọn file .csv");
      }
    }
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleFileSelect = (e) => {
    const file = e.target.files?.[0];
    if (file && file.name.endsWith(".csv")) {
      onUpload(file);
    } else {
      alert("Vui lòng chọn file .csv");
    }
  };

  const handleClick = () => {
    fileInputRef.current?.click();
  };

  return (
    <div className="upload-section">
      <div
        className="upload-box"
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onClick={handleClick}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv"
          onChange={handleFileSelect}
          style={{ display: "none" }}
          disabled={loading}
        />
        <div className="upload-icon">📁</div>
        <h2>Kéo & Thả File CSV Tại Đây</h2>
        <p>hoặc nhấp để chọn file</p>
      </div>
    </div>
  );
}
