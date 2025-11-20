import { useEffect, useState } from 'react';
import axios from 'axios';

const entities = [
  { label: 'Sách', value: 'BOOK' },
  { label: 'Khách hàng', value: 'CUSTOMER' },
  { label: 'Đơn hàng', value: 'ORDER' },
  { label: 'Giỏ hàng', value: 'CART' },
  { label: 'Hóa đơn', value: 'INVOICE' }
];

function SummaryCard({ summary }) {
  return (
    <div className="card">
      <h2>ETL Dashboard</h2>
      <ul>
        <li><span>Tổng bản ghi đã kiểm tra</span><strong>{summary.totalProcessed}</strong></li>
        <li><span>Tổng bản ghi staging</span><strong>{summary.totalStaging}</strong></li>
        <li><span>PASS</span><strong className="text-success">{summary.passed}</strong></li>
        <li><span>FIXED</span><strong className="text-warning">{summary.fixed}</strong></li>
        <li><span>FAILED</span><strong className="text-danger">{summary.failed}</strong></li>
        <li><span>Lần chạy gần nhất</span><strong>{summary.lastRun ?? 'Chưa có dữ liệu'}</strong></li>
      </ul>
    </div>
  );
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
                  <th key={key}>{key}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row, idx) => (
                <tr key={idx} onClick={() => onSelect?.(row)}>
                  {Object.keys(row).map((key) => (
                    <td key={key}>{String(row[key])}</td>
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
        <button className="close" onClick={onClose}>×</button>
        <h3>Chi tiết bản ghi #{record.entityKey}</h3>
        <section>
          <h4>Raw Data</h4>
          <pre>{JSON.stringify(record.rawData, null, 2)}</pre>
        </section>
        <section>
          <h4>DQ Result</h4>
          <pre>{JSON.stringify(record.dqResult, null, 2)}</pre>
        </section>
        <section>
          <h4>Transform/Staging</h4>
          <pre>{JSON.stringify(record.stagingData, null, 2)}</pre>
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
    lastRun: null
  });
  const [entity, setEntity] = useState('BOOK');
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
    const { data } = await axios.get('/api/etl/dashboard');
    setSummary({
      ...data,
      lastRun: data.lastRun ? new Date(data.lastRun).toLocaleString('vi-VN') : null
    });
  };

  const loadTransformed = async (entityType) => {
    const { data } = await axios.get('/api/etl/dashboard/transformed', { params: { entity: entityType } });
    setTransformedData(data);
  };

  const loadErrors = async () => {
    const { data } = await axios.get('/api/etl/dashboard/errors');
    setErrorData(data);
  };

  const openDetail = async (row) => {
    const entityType = determineEntity(row);
    if (!entityType) return;
    const entityKey = row.book_key || row.customer_key || row.order_key || row.entity_key;
    const { data } = await axios.get(`/api/etl/dashboard/records/${entityType}/${entityKey}`);
    setDetail(data);
  };

  const determineEntity = (row) => {
    if (row.book_key || row.ma_sach) return 'BOOK';
    if (row.customer_key || row.ma_nguoi_dung) return 'CUSTOMER';
    if (row.order_key || row.ma_don_hang) return 'ORDER';
    if (row.cart_key || row.ma_gio_hang) return 'CART';
    if (row.invoice_key || row.ma_hoa_don) return 'INVOICE';
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
            className={entity === opt.value ? 'active' : ''}
            onClick={() => setEntity(opt.value)}
          >
            {opt.label}
          </button>
        ))}
        <button onClick={loadSummary}>↻ Refresh</button>
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
