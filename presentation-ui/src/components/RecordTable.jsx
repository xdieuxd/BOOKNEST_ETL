import React from "react";
import "./RecordTable.css";

function friendlyHeader(key) {
  const map = {
    book_key: "MA SACH",
    title: "TIEU DE",
    authors: "TAC GIA",
    categories: "THE LOAI",
    description: "MO TA",
    price: "GIA",
    customer_key: "MA KHACH",
    full_name: "HO TEN",
    email: "EMAIL",
    phone: "SO DIEN THOAI",
    order_key: "MA DON",
    status: "TRANG THAI",
    entity_type: "LOAI",
    entity_key: "MA",
    errors: "LOI",
    checked_at: "THOI GIAN",
    loaded_at: "THOI DIEM TAI",
  };
  if (map[key]) return map[key];
  return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

export default function RecordTable({
  title,
  data = [],
  color = "default",
  onSelect,
}) {
  return (
    <div className={`panel panel-${color}`}>
      <div className="panel-header">
        <h3>{title}</h3>
      </div>
      <div className="panel-body">
        {data.length === 0 ? (
          <p className="muted">Chưa có dữ liệu</p>
        ) : (
          <table>
            <thead>
              <tr>
                {Object.keys(data[0]).map((key) => (
                  <th key={key}>{friendlyHeader(key)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row, idx) => (
                <tr key={idx} onClick={() => onSelect?.(row)}>
                  {Object.keys(row).map((key) => (
                    <td key={key}>
                      {row[key] === null || row[key] === undefined
                        ? ""
                        : String(row[key])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
