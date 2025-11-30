import React from "react";
import "./RecordTable.css";

function friendlyHeader(key) {
  const map = {
    book_key: "Mã sách",
    title: "Tiêu đề",
    authors: "Tác giả",
    categories: "Thể loại",
    description: "Mô tả",
    price: "Giá",
    customer_key: "Mã khách",
    full_name: "Họ tên",
    email: "Email",
    phone: "SĐT",
    order_key: "Mã đơn",
    status: "Trạng thái",
    entity_type: "Loại",
    entity_key: "Mã",
    errors: "Lỗi",
    checked_at: "Thời gian",
    loaded_at: "Thời điểm tải",
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
