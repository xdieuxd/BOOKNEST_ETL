# ETL Presentation UI

Giao diện trình bày kết quả ETL Pipeline - dành riêng cho demo và báo cáo.

## Tính năng

- ✅ Upload file CSV (drag & drop hoặc click)
- ✅ Hiển thị luồng xử lý trực quan (Extract → DQ → Transform → Load)
- ✅ Thống kê stats: Passed, Failed, Fixable
- ✅ Xem dữ liệu đã transform thành công
- ✅ Xem chi tiết lỗi validation (bảng đỏ)
- ✅ Chỉnh sửa dữ liệu lỗi + reprocess
- ✅ Tải xuống kết quả

## Cài đặt & Chạy

### Yêu cầu

- Node.js >= 16
- npm hoặc yarn

### Bước 1: Cài dependencies

```bash
cd presentation-ui
npm install
```

### Bước 2: Khởi động dev server

```bash
npm run dev
```

Truy cập: `http://localhost:3000`

### Bước 3: Khởi động backend

```bash
# Từ thư mục gốc
docker compose build
docker compose up -d
```

Backend sẽ chạy trên `http://localhost:8080`

## Build Production

```bash
npm run build
# Output: dist/
```

## Cấu trúc

```
presentation-ui/
├── src/
│   ├── components/
│   │   ├── UploadSection.jsx       # Upload CSV
│   │   ├── PipelineVisualization.jsx # Pipeline flow
│   │   └── ResultsPanel.jsx        # Results & errors
│   ├── App.jsx
│   ├── App.css
│   └── main.jsx
├── index.html
├── vite.config.js
└── package.json
```

## API Endpoints

### Upload CSV

```
POST /api/etl/upload
Content-Type: multipart/form-data

Response:
{
  "extract": { "totalRecords": 10 },
  "dq": { "passed": 8, "failed": 2, "fixable": 1 },
  "transform": { "processed": 8 },
  "load": { "loaded": 8 },
  "results": {
    "transformed": [...],
    "errors": [...]
  }
}
```

### Reprocess Fixed Data

```
POST /api/etl/reprocess
Content-Type: application/json

Body: { corrected data }
```

## Lưu ý

- Giao diện này là dành cho **trình bày kết quả**, không dùng để debug nội bộ
- Debug dashboard vẫn ở `dashboard-ui/`
