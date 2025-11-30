import { useEffect, useState } from "react";
import axios from "axios";

const entities = [
  { label: "Sách", value: "BOOK" },
  { label: "Khách hàng", value: "CUSTOMER" },
  { label: "Đơn hàng", value: "ORDER" },
  { label: "Giỏ hàng", value: "CART" },
  { label: "Hóa đơn", value: "INVOICE" },
];

function SummaryCard({ summary }) {
  return (
    <div className="card summary-card">
      <div className="summary-header">
        <h1>Bảng điều khiển ETL</h1>
        <p className="muted">
          Tổng quan trạng thái pipeline và kết quả kiểm tra dữ liệu
        </p>
      </div>

      <div className="summary-grid">
        <div className="stat">
          <div className="stat-label">Tổng bản ghi đã kiểm tra</div>
          <div className="stat-value">{summary.totalProcessed}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Tổng bản ghi staging</div>
          <div className="stat-value">{summary.totalStaging}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Đã qua</div>
          <div className="stat-value text-success">{summary.passed}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Đã sửa</div>
          <div className="stat-value text-warning">{summary.fixed}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Bị lỗi</div>
          <div className="stat-value text-danger">{summary.failed}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Lần chạy gần nhất</div>
          <div className="stat-value">
            {summary.lastRun ?? "Chưa có dữ liệu"}
          </div>
        </div>
      </div>
    </div>
  );
}

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

function RecordTable({ title, data, color, onSelect }) {
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

function DetailModal({ record, onClose }) {
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

export default function App() {
  const [summary, setSummary] = useState({
    totalProcessed: 0,
    totalStaging: 0,
    passed: 0,
    fixed: 0,
    failed: 0,
    lastRun: null,
  });
  const [entity, setEntity] = useState("BOOK");
  const [transformedData, setTransformedData] = useState([]);
  const [errorData, setErrorData] = useState([]);
  const [detail, setDetail] = useState(null);

  useEffect(() => {
    loadSummary();
    loadErrors();
  }, []);

  useEffect(() => {
    loadTransformed(entity);
  }, [entity]);

  const loadSummary = async () => {
    const { data } = await axios.get("/api/etl/dashboard");
    setSummary({
      ...data,
      lastRun: data.lastRun
        ? new Date(data.lastRun).toLocaleString("vi-VN")
        : null,
    });
  };

  const loadTransformed = async (entityType) => {
    const { data } = await axios.get("/api/etl/dashboard/transformed", {
      params: { entity: entityType },
    });
    setTransformedData(data);
  };

  const loadErrors = async () => {
    const { data } = await axios.get("/api/etl/dashboard/errors");
    setErrorData(data);
  };

  const openDetail = async (row) => {
    const entityType = determineEntity(row);
    if (!entityType) return;
    const entityKey =
      row.book_key || row.customer_key || row.order_key || row.entity_key;
    const { data } = await axios.get(
      `/api/etl/dashboard/records/${entityType}/${entityKey}`
    );
    setDetail(data);
  };

  const determineEntity = (row) => {
    if (row.book_key || row.ma_sach) return "BOOK";
    if (row.customer_key || row.ma_nguoi_dung) return "CUSTOMER";
    if (row.order_key || row.ma_don_hang) return "ORDER";
    if (row.cart_key || row.ma_gio_hang) return "CART";
    if (row.invoice_key || row.ma_hoa_don) return "INVOICE";
    if (row.entity_type) return row.entity_type;
    return null;
  };

  return (
    <div className="container">
      <SummaryCard summary={summary} />

      <div className="actions">
        {entities.map((opt) => (
          <button
            key={opt.value}
            className={entity === opt.value ? "active" : ""}
            onClick={() => setEntity(opt.value)}
          >
            {opt.label}
          </button>
        ))}
        <button onClick={loadSummary}>↻ Làm mới</button>
      </div>

      <div className="grid">
        <RecordTable
          title="Dữ liệu đã Transform (PASS/FIXED)"
          data={transformedData}
          color="success"
          onSelect={openDetail}
        />
        <RecordTable
          title="Lỗi Data Quality"
          data={errorData}
          color="danger"
          onSelect={openDetail}
        />
      </div>

      <DetailModal record={detail} onClose={() => setDetail(null)} />
    </div>
  );
}
