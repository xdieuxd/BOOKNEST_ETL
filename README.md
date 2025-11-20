# Booknest ETL Platform

Pipeline ETL mô phỏng hệ thống bán sách gồm:

* **Nguồn dữ liệu**: MySQL (`source_db`) + các file CSV.
* **Luồng xử lý**: Extract → RabbitMQ → Data Quality (Chain of Responsibility + Regex) → Transform → Load vào các bảng staging.
* **Monitoring**: REST API + React dashboard xem trạng thái, dữ liệu đã transform, lỗi từng rule, chi tiết bản ghi.

## 1. Chuẩn bị môi trường

### Yêu cầu
* Docker Desktop
* Node.js >= 18 (để chạy dashboard)
* Maven 3.x (nếu muốn build ngoài container)

### Khởi động hạ tầng + backend
```bash
docker compose up --build
```
Điều này build service Spring Boot, chạy MySQL và RabbitMQ. Sau khi lên, seed schema + dữ liệu mẫu bằng:
```bash
Get-Content .\scripts\seed-source-staging.sql | docker compose exec -T mysql-source sh -c "mysql -uroot -proot"
```

### Chạy React dashboard
```bash
cd dashboard-ui
npm install
npm run dev
```
Ứng dụng mở tại http://localhost:5173 và proxy về API backend (cổng 8080).

## 2. Các endpoint chính
| API | Mục đích |
| --- | --- |
| `POST /api/etl/run/database` | chạy extract từ MySQL |
| `POST /api/etl/run/csv` | chạy extract từ CSV |
| `POST /api/etl/run/all` | chạy cả hai nguồn |
| `GET /api/etl/dashboard` | dữ liệu cho thẻ thống kê |
| `GET /api/etl/dashboard/transformed?entity=BOOK` | dữ liệu đã transform (BOOK/CUSTOMER/ORDER/CART/INVOICE) |
| `GET /api/etl/dashboard/errors` | danh sách lỗi theo rule |
| `GET /api/etl/dashboard/records/{type}/{id}` | popup chi tiết raw + DQ + staging |

## 3. Kiến trúc chính
* **Extract**: `CsvExtractService`, `DatabaseExtractService`, `EtlOrchestratorService`.
* **Messaging**: RabbitMQ exchange `etl.exchange` với queue raw/quality/error.
* **Data Quality**: `DataQualityService` + bộ rule (`NotBlankRule`, `RegexRule`, `MaxLengthRule`...). Kết quả được ghi vào `dq_result`.
* **Transform**: `TransformService` chuẩn hóa chuỗi, email, phone, tính lại tổng tiền...
* **Load**: `StagingLoaderService` đổ vào `stg_books`, `stg_customers`, `stg_orders`, `stg_order_items`, `stg_carts`, `stg_cart_items`, `stg_invoices`, ghi log vào `etl_log`.
* **Monitoring**: `DashboardController` cung cấp API trạng thái, dữ liệu transform, lỗi và popup chi tiết.

## 4. Các bảng xử lý
* Nguồn: `sach`, `nguoi_dung`, `don_hang`, `chi_tiet_don_hang`, `gio_hang`, `chi_tiet_gio_hang`, `hoa_don`, các file CSV (books, orders, customers, order_items).
* Staging: `stg_books`, `stg_customers`, `stg_orders`, `stg_order_items`, `stg_carts`, `stg_cart_items`, `stg_invoices`.

## 5. Dashboard React
* Thẻ thống kê: tổng bản ghi đọc, staging, PASS/FIXED/FAILED, thời gian chạy gần nhất.
* Hai bảng song song (màu xanh cho PASS/FIXED, màu đỏ cho lỗi).
* Popup chi tiết: raw data – DQ result – dữ liệu staging sau transform.

## 6. Tuỳ chỉnh rule
Các rule được định nghĩa trong `DataQualityService`. Muốn thêm/đổi thông báo:
```java
.addRule(new NotBlankRule<>("fullName", "Họ tên không được trống", UserRawMessage::getFullName))
.addRule(new RegexRule<>("email", "Email không hợp lệ", EMAIL_PATTERN, UserRawMessage::getEmail))
```
Sử dụng `DataQualityRuleChain` để xây dựng chuỗi xử lý tùy ý.
