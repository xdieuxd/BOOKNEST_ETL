import React, { useEffect, useRef, useState } from "react";

const EditForm = React.forwardRef(
  ({ rowId, errors, editingData, onResult, onCancel }, ref) => {
    const inputRefs = useRef({});
    const [localValues, setLocalValues] = useState({});

    useEffect(() => {
      const source = editingData || {};
      const init = {};
      (errors || []).forEach((err) => {
        init[err.field] = source[err.field] || "";
      });
      setLocalValues(init);
      if (errors && errors.length > 0) {
        const first = errors[0].field;
        setTimeout(() => {
          if (inputRefs.current[first]) inputRefs.current[first].focus();
        }, 0);
      }
    }, [rowId, errors]);

    const handleInputChange = (field, value) => {
      setLocalValues((prev) => ({ ...prev, [field]: value }));
    };

    const handleSave = async () => {
      console.log("[DEBUG] Nhấn Lưu Sửa");
      const source = editingData || {};
      let payload = { ...source, ...localValues };
      // Nếu trường lỗi là email, đảm bảo cả hai key đều có trong payload
      if (payload.customer_email) {
        payload.email = payload.customer_email;
      } else if (payload.email) {
        payload.customer_email = payload.email;
      }
      console.log("[DEBUG] Payload gửi lên API:", payload);
      try {
        const resp = await fetch("/api/etl/reprocess", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        console.log("[DEBUG] Response trả về từ API:", data);
        if (!resp.ok) throw new Error(data.message || "Reprocess failed");
        if (
          !data.results ||
          (!data.results.transformed && !data.results.errors)
        ) {
          alert(
            "API không trả về đúng định dạng kết quả. Vui lòng kiểm tra lại backend."
          );
          return;
        }
        if (onResult) onResult(data.results);
      } catch (err) {
        console.error("Reprocess error", err);
        alert("Có lỗi khi lưu: " + (err.message || err));
      }
    };

    return (
      <div className="edit-form">
        <h4>Sửa các trường có lỗi ({errors.length})</h4>
        <div className="edit-fields">
          {errors && errors.length > 0 ? (
            errors.map((err, i) => (
              <div
                key={`field-${rowId}-${err.field}`}
                className="edit-field-group"
              >
                <label htmlFor={`input-${rowId}-${err.field}`}>
                  {err.field} <span className="required">*</span>
                </label>
                <div className="error-info">
                  <span className="error-rule">{err.rule}</span>
                  <span className="error-message">{err.message}</span>
                </div>
                <input
                  id={`input-${rowId}-${err.field}`}
                  ref={(el) => {
                    if (el) inputRefs.current[err.field] = el;
                  }}
                  type="text"
                  value={localValues[err.field] || ""}
                  onChange={(e) => handleInputChange(err.field, e.target.value)}
                  className="edit-input-field"
                  placeholder={`Nhập giá trị mới cho ${err.field}...`}
                  autoComplete="off"
                  spellCheck="false"
                />
              </div>
            ))
          ) : (
            <p className="no-errors">Không có lỗi</p>
          )}
        </div>
        <div className="edit-actions">
          <button className="btn-save" onClick={handleSave}>
            Lưu Sửa
          </button>
          <button className="btn-cancel" onClick={onCancel}>
            Hủy
          </button>
        </div>
      </div>
    );
  }
);

EditForm.displayName = "EditForm";

export default React.memo(EditForm);
