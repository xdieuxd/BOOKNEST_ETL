import React, {
  Fragment,
  useState,
  useCallback,
  useRef,
  useEffect,
} from "react";
import axios from "axios";
import EditForm from "./EditForm";
import "./ResultsPanel.css";

export default function ResultsPanel({ state, onReprocess }) {
  const [activeTab, setActiveTab] = useState("transformed");
  const [editingRowId, setEditingRowId] = useState(null);
  const [editingData, setEditingData] = useState({});
  const [editingRowErrors, setEditingRowErrors] = useState([]);

  const [localTransformed, setLocalTransformed] = useState(
    state?.results?.transformed || []
  );
  const [localErrors, setLocalErrors] = useState(state?.results?.errors || []);

  const scrollPositionRef = useRef(0);
  const tableWrapperRef = useRef(null);

  React.useEffect(() => {
    setLocalTransformed(state?.results?.transformed || []);
    setLocalErrors(state?.results?.errors || []);
  }, [state?.results]);

  const saveScrollPosition = useCallback(() => {
    if (tableWrapperRef.current) {
      scrollPositionRef.current = tableWrapperRef.current.scrollTop;
    }
  }, []);

  useEffect(() => {
    if (editingRowId && tableWrapperRef.current) {
      const timer = setTimeout(() => {
        tableWrapperRef.current.scrollTop = scrollPositionRef.current;
      }, 0);
      return () => clearTimeout(timer);
    }
  }, [editingRowId, localErrors, localTransformed]);

  const handleEditStart = useCallback(
    (rowId, rowData) => {
      saveScrollPosition();
      setEditingRowId(rowId);
      const cloned = {};
      Object.keys(rowData || {}).forEach((k) => {
        const v = rowData[k];
        if (!k.startsWith("_")) {
          cloned[k] = v === null || v === undefined ? "" : String(v);
        }
      });
      setEditingData(cloned);
      setEditingRowErrors(rowData._errors || []);
    },
    [saveScrollPosition]
  );

  const handleEditChange = useCallback((field, value) => {
    setEditingData((prev) => ({ ...prev, [field]: value }));
  }, []);

  const isValidEmail = (email) => {
    if (!email || email.trim() === "") return false;
    const emailRegex = /^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/;
    return emailRegex.test(email);
  };

  const isValidPrice = (price) => {
    if (!price || price.trim() === "") return true;
    const priceRegex = /^[0-9]+(\.[0-9]{1,2})?$/;
    return priceRegex.test(price);
  };

  const isValidFullName = (name) => {
    return name && name.trim() !== "";
  };

  const handleSave = async () => {
    saveScrollPosition();
    for (const err of editingRowErrors) {
      const field = err.field;
      const value = editingData[field] || "";

      if (field === "full_name" || field === "fullName") {
        if (!isValidFullName(value)) {
          alert(`Trường "${field}" không được để trống`);
          return;
        }
      } else if (field === "email") {
        if (!isValidEmail(value)) {
          alert(`Email "${value}" không hợp lệ. Định dạng: user@gmail.com`);
          return;
        }
      } else if (field === "price") {
        if (!isValidPrice(value)) {
          alert(`Giá "${value}" không hợp lệ. Định dạng: 45000 hoặc 45000.50`);
          return;
        }
      } else if (field === "title" || field === "description") {
        if (value.length > 300) {
          alert(`Trường "${field}" vượt quá 300 ký tự!`);
          return;
        }
      } else if (field === "authors") {
        // Chuẩn hóa authors: viết hoa chữ cái đầu mỗi từ
        editingData[field] = value.replace(/\b\w/g, (c) => c.toUpperCase());
      }
    }

    // Call reprocess with edited data
    try {
      const response = await axios.post("/api/etl/reprocess", editingData);
      const { results } = response.data;

      // Find current error row index
      const errorRowIndex = localErrors.findIndex(
        (row) =>
          (row.id || row.book_id || row.customer_id || row.customerId) ===
          editingRowId
      );

      if (errorRowIndex >= 0) {
        if (results.transformed.length > 0) {
          // Row is now fixed - move to transformed list
          const fixedRow = results.transformed[0];
          setLocalTransformed((prev) => [...prev, fixedRow]);
          setLocalErrors((prev) => prev.filter((_, i) => i !== errorRowIndex));
          setEditingRowId(null);
          setEditingData({});
          setEditingRowErrors([]);
          alert("Sửa thành công!");
        } else if (results.errors.length > 0) {
          // Row still has errors - update local errors with remaining ones
          const updatedErrorRow = results.errors[0];
          setLocalErrors((prev) => {
            const newErrors = [...prev];
            newErrors[errorRowIndex] = updatedErrorRow;
            return newErrors;
          });

          // Update editing state to show remaining errors
          setEditingRowErrors(updatedErrorRow._errors || []);
          alert("Còn lỗi.");
        }
      }
    } catch (err) {
      console.error("Lỗi khi gọi reprocess:", err);
      alert("Có lỗi xảy ra khi xử lý lại dữ liệu");
    }
  };

  const handleCancel = useCallback(() => {
    setEditingRowId(null);
    setEditingData({});
    setEditingRowErrors([]);
  }, []);

  const DataTable = ({ data, isErrors }) => {
    if (!data || data.length === 0) {
      return <p className="no-data">Không có dữ liệu</p>;
    }

    // Exclude _errors and _original_* columns from display columns
    const columns = data[0]
      ? Object.keys(data[0])
          .filter((k) => k !== "_errors" && !k.startsWith("_original_"))
          .slice(0, 6)
      : [];

    // Hàm chuẩn hóa tên
    const normalizeName = (name) => {
      if (!name) return "";
      // Chuyển các tên tắt thành tên đầy đủ (ví dụ: ng t. Hà -> Nguyễn Thị Hà)
      let result = name.trim();
      result = result.replace(/ng\s*t\.?/i, "Nguyễn Thị");
      result = result.replace(/le\s*m\.?/i, "Lê Minh");
      // Viết hoa chữ cái đầu mỗi từ
      result = result.replace(/\b\w/g, (c) => c.toUpperCase());
      return result;
    };
    // Hàm chuẩn hóa status
    const normalizeStatus = (status) => {
      if (!status) return "";
      return status
        .trim()
        .replace(/_/g, " ")
        .replace(/\b\w/g, (c) => c.toUpperCase());
    };

    return (
      <div className="table-wrapper" ref={tableWrapperRef}>
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col}>{col.replace(/_/g, " ").toUpperCase()}</th>
              ))}
              {isErrors && <th>Lỗi Validation</th>}
              {isErrors && <th>Hành Động</th>}
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
              const isEditing = editingRowId === rowId;

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
                      if (
                        col === "full_name" ||
                        col === "fullName" ||
                        col === "title" ||
                        col === "customer_name"
                      )
                        transformedValue = normalizeName(transformedValue);
                      if (col === "status")
                        transformedValue = normalizeStatus(transformedValue);
                      if (col === "email" || col === "customer_email")
                        transformedValue = transformedValue.toLowerCase();
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

                    {isErrors && (
                      <td className="action-cell">
                        {!isEditing && (
                          <button
                            className="btn-edit"
                            onClick={() => handleEditStart(rowId, row)}
                          >
                            Sửa
                          </button>
                        )}
                      </td>
                    )}
                  </tr>

                  {isEditing && isErrors && (
                    <tr className="edit-form-row" key={`edit-${rowId}`}>
                      <td
                        colSpan={columns.length + 2}
                        className="edit-form-cell"
                      >
                        <EditForm
                          rowId={rowId}
                          errors={editingRowErrors}
                          editingData={editingData}
                          onResult={(results) => {
                            // Tìm index theo rowId thay vì editingRowId
                            const errorRowIndex = localErrors.findIndex(
                              (row) =>
                                (row.id ||
                                  row.book_id ||
                                  row.customer_id ||
                                  row.customerId) === rowId
                            );
                            if (errorRowIndex >= 0) {
                              if (
                                results.transformed &&
                                results.transformed.length > 0
                              ) {
                                const fixedRow = results.transformed[0];
                                setLocalTransformed((prev) => [
                                  ...prev,
                                  fixedRow,
                                ]);
                                setLocalErrors((prev) =>
                                  prev.filter((_, i) => i !== errorRowIndex)
                                );
                                setEditingRowId(null);
                                setEditingData({});
                                setEditingRowErrors([]);
                                alert("Sửa thành công!");
                              } else if (
                                results.errors &&
                                results.errors.length > 0
                              ) {
                                const updatedErrorRow = results.errors[0];
                                setLocalErrors((prev) => {
                                  const newErrors = [...prev];
                                  newErrors[errorRowIndex] = updatedErrorRow;
                                  return newErrors;
                                });
                                setEditingRowErrors(
                                  updatedErrorRow._errors || []
                                );
                                alert("Còn lỗi.");
                              }
                            } else {
                              // Nếu không tìm thấy index, cập nhật lại toàn bộ bảng từ kết quả trả về
                              setLocalTransformed(results.transformed || []);
                              setLocalErrors(results.errors || []);
                              setEditingRowId(null);
                              setEditingData({});
                              setEditingRowErrors([]);
                              alert("Sửa thành công!");
                            }
                          }}
                          onCancel={handleCancel}
                        />
                      </td>
                    </tr>
                  )}
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

  return (
    <div className="results-panel">
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
            if (editingRowId) {
              alert(
                "Vui lòng lưu các chỉnh sửa đang mở trước khi load vào DB."
              );
              return;
            }

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
            if (editingRowId) {
              alert("Vui lòng lưu các chỉnh sửa đang mở trước khi tải xuống.");
              return;
            }

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
