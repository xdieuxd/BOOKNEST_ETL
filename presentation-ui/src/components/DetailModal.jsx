import React from "react";
import "./DetailModal.css";

export default function DetailModal({ record, onClose }) {
  if (!record) return null;
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <button className="close" onClick={onClose}>
          Đóng
        </button>
        <h3>Chi tiết bản ghi</h3>
        <p className="muted">
          Mã: {record.entityKey || record.id || record.book_key}
        </p>
        <section>
          <h4>Dữ liệu thô</h4>
          <pre>{JSON.stringify(record.rawData || record, null, 2)}</pre>
        </section>
        <section>
          <h4>Kết quả DQ</h4>
          <pre>
            {JSON.stringify(record.dqResult || record.errors || null, null, 2)}
          </pre>
        </section>
        <section>
          <h4>Transform / Staging</h4>
          <pre>{JSON.stringify(record.stagingData || record, null, 2)}</pre>
        </section>
      </div>
    </div>
  );
}
