import React, { Fragment, useState, useRef } from "react";
import axios from "axios";
import "./ResultsPanel.css";

export default function ResultsPanel({ state, onRefresh, activeEntity }) {
  const [activeTab, setActiveTab] = useState("transformed");

  // Get data for active entity only
  const getTransformedData = () => {
    if (state?.results?.byEntity?.transformed && activeEntity) {
      return state.results.byEntity.transformed[activeEntity] || [];
    }
    return state?.results?.transformed || [];
  };

  const getErrorsData = () => {
    if (state?.results?.byEntity?.errors && activeEntity) {
      return state.results.byEntity.errors[activeEntity] || [];
    }
    return state?.results?.errors || [];
  };

  const [localTransformed, setLocalTransformed] = useState(
    getTransformedData()
  );
  const [localErrors, setLocalErrors] = useState(getErrorsData());

  const tableWrapperRef = useRef(null);

  React.useEffect(() => {
    setLocalTransformed(getTransformedData());
    setLocalErrors(getErrorsData());
  }, [state?.results, activeEntity]);

  const DataTable = ({ data, isErrors }) => {
    if (!data || data.length === 0) {
      return <p className="no-data">Không có dữ liệu</p>;
    }

    // Exclude _errors and _original_* columns from display columns
    const columns = data[0]
      ? Object.keys(data[0])
          .filter(
            (k) =>
              k !== "_errors" &&
              !k.startsWith("_original_") &&
              k !== "_status" &&
              k !== "_error_message"
          )
          .slice(0, 6)
      : [];

    // Backend đã xử lý normalization, frontend chỉ hiển thị

    return (
      <div className="table-wrapper" ref={tableWrapperRef}>
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col}>{col.replace(/_/g, " ").toUpperCase()}</th>
              ))}
              {isErrors && <th>Lỗi Validation</th>}
              <th>Trạng Thái</th>
            </tr>
          </thead>
          <tbody>
            {data.map((row, idx) => {
              const rowId =
                row.id ||
                row.book_id ||
                row.customer_id ||
                row.customerId ||
                idx;

              return (
                <Fragment key={`row-${rowId}-${idx}`}>
                  <tr className={isErrors ? "error-row" : "success-row"}>
                    {columns.map((col) => {
                      // Chỉ hiển thị giá trị gốc cho các cột cần chuẩn hóa
                      const showOriginal = [
                        "full_name",
                        "fullName",
                        "status",
                        "email",
                        "phone",
                        "title",
                        "customer_name",
                        "customer_email",
                      ].includes(col);
                      const originalKey = `_original_${col}`;
                      const hasOriginal = row.hasOwnProperty(originalKey);
                      const originalValue = row[originalKey];
                      let transformedValue = String(row[col] || "");
                      // Hiển thị nguyên giá trị từ backend, không transform lại
                      // Backend đã xử lý normalization đúng với Unicode tiếng Việt
                      const isDifferent =
                        hasOriginal && originalValue !== row[col];

                      return (
                        <td key={`${rowId}-${col}`}>
                          {showOriginal && hasOriginal && isDifferent ? (
                            <div className="value-comparison">
                              <div
                                className="original-value"
                                title="Giá trị gốc"
                              >
                                <span className="label">Gốc:</span>{" "}
                                {String(originalValue || "")}
                              </div>
                              <div
                                className={`transformed-value changed`}
                                title="Giá trị đã transform"
                              >
                                <span className="label">→</span>{" "}
                                {transformedValue}
                              </div>
                            </div>
                          ) : (
                            <span>{transformedValue}</span>
                          )}
                        </td>
                      );
                    })}

                    {isErrors && (
                      <td className="errors-cell">
                        <ErrorDisplay errors={row._errors} />
                      </td>
                    )}

                    <td className="status-cell">
                      {row._status === "SENT_TO_RABBITMQ" && (
                        <span className="status-badge processing">
                          ⏳ Processing...
                        </span>
                      )}
                      {row._status === "PARSE_ERROR" && (
                        <span className="status-badge error">
                          ❌ Parse Error
                        </span>
                      )}
                      {!row._status && isErrors && (
                        <span className="status-badge error">
                          ❌ Validation Failed
                        </span>
                      )}
                      {!row._status && !isErrors && (
                        <span className="status-badge success">✅ Success</span>
                      )}
                    </td>
                  </tr>
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  };

  const ErrorDisplay = ({ errors }) => {
    if (!errors) return <span className="no-errors">✓ OK</span>;

    let errorList = errors;
    if (typeof errors === "string") {
      try {
        errorList = JSON.parse(errors);
      } catch {
        return <span className="error-text">{errors}</span>;
      }
    }

    if (!Array.isArray(errorList)) {
      errorList = [errorList];
    }

    return (
      <div className="error-details">
        {errorList.map((err, i) => (
          <div key={i} className="error-item">
            <span className="error-field">{err.field || "field"}</span>
            <span className="error-rule">{err.rule || ""}</span>
            <span className="error-message">{err.message || ""}</span>
          </div>
        ))}
      </div>
    );
  };

  const entityLabels = {
    books: "📚 Sách",
    customers: "👤 Khách hàng",
    orders: "🛒 Đơn hàng",
    order_items: "📦 Chi tiết đơn",
    carts: "🛍️ Giỏ hàng",
    invoices: "🧾 Hóa đơn",
  };

  return (
    <div className="results-panel">
      <div className="panel-header">
        <h3>📝 Xem Chi Tiết - {entityLabels[activeEntity] || activeEntity}</h3>
        <button
          className="btn-refresh"
          onClick={onRefresh}
          style={{
            padding: "8px 16px",
            background: "#4CAF50",
            color: "white",
            border: "none",
            borderRadius: "4px",
            cursor: "pointer",
            fontSize: "14px",
          }}
        >
          🔄 Refresh Results from Staging DB
        </button>
      </div>
      <div className="tabs">
        <button
          className={`tab-btn ${activeTab === "transformed" ? "active" : ""}`}
          onClick={() => setActiveTab("transformed")}
        >
          Dữ Liệu Đã Transform ({localTransformed.length})
        </button>
        <button
          className={`tab-btn ${activeTab === "errors" ? "active" : ""}`}
          onClick={() => setActiveTab("errors")}
        >
          Lỗi Validation ({localErrors.length})
        </button>
      </div>

      <div className="tab-content">
        {activeTab === "transformed" && (
          <div className="tab-pane">
            <h3>Dữ Liệu Đã Transform Thành Công ({localTransformed.length})</h3>
            <DataTable data={localTransformed} isErrors={false} />
          </div>
        )}

        {activeTab === "errors" && (
          <div className="tab-pane">
            <h3>Dữ Liệu Có Lỗi Validation ({localErrors.length})</h3>
            {localErrors.length > 0 ? (
              <>
                <DataTable data={localErrors} isErrors={true} />
              </>
            ) : (
              <p className="no-data">Tất cả dữ liệu đều hợp lệ!.</p>
            )}
          </div>
        )}
      </div>

      <div className="results-footer">
        {localErrors.length === 0 && (
          <p className="success-msg">
            ✅ Tất cả dữ liệu đã hợp lệ! Bạn có thể load vào DB chính hoặc tải
            xuống CSV.
          </p>
        )}
        <button
          className="btn-load-db"
          disabled={localErrors.length > 0}
          onClick={async () => {
            if (
              !window.confirm(
                "Xác nhận load dữ liệu vào database chính (source_db)?"
              )
            ) {
              return;
            }

            try {
              const resp = await axios.post("/api/etl/load-to-source");
              const { loaded } = resp.data;
              alert(
                `✅ Load thành công!\n` +
                  `- Customers: ${loaded.customers}\n` +
                  `- Books: ${loaded.books}\n` +
                  `- Orders: ${loaded.orders}\n` +
                  `Tổng: ${loaded.total} records đã được lưu vào source_db`
              );
            } catch (err) {
              console.error("Lỗi khi load vào DB:", err);
              alert(
                "❌ Không thể load dữ liệu vào DB chính: " +
                  (err.response?.data?.message || err.message)
              );
            }
          }}
        >
          {localErrors.length > 0 ? "❌ Còn lỗi" : "💾 Load Vào DB Chính"}
        </button>

        <button
          className="btn-export"
          disabled={localErrors.length > 0}
          onClick={async () => {
            const rows = [...localTransformed, ...localErrors];

            try {
              const resp = await axios.post(
                "/api/etl/save",
                { rows },
                { responseType: "blob" }
              );
              const blob = new Blob([resp.data], {
                type: "text/csv;charset=utf-8;",
              });
              const url = window.URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = "cleaned_results.csv";
              document.body.appendChild(a);
              a.click();
              a.remove();
              window.URL.revokeObjectURL(url);
            } catch (err) {
              console.error("Lỗi khi tải xuống:", err);
              alert("Không thể tải xuống kết quả.");
            }
          }}
        >
          {localErrors.length > 0 ? "Còn lỗi" : "📥 Tải Xuống CSV"}
        </button>

        <button className="btn-retry" onClick={() => window.location.reload()}>
          🔄 Tải Lại
        </button>
      </div>
    </div>
  );
}
