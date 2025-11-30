# 🚀 Hướng Dẫn Start Demo - BookNest ETL

## 📋 Quy Trình Khởi Động

### **Option 1: Chạy với Docker (Khuyến Nghị)** 🐳

#### **Khi CÓ thay đổi code backend:**

```powershell
# Bước 1: Build lại ứng dụng
cd E:\BOOKNEST_ETL
mvn clean package -DskipTests

# Bước 2: Build lại Docker image (không cache)
docker-compose build --no-cache

# Bước 3: Dừng và xóa containers cũ
docker-compose down

# Bước 4: Khởi động tất cả services
docker-compose up -d

# Bước 5: Xem logs real-time
docker logs -f booknest-etl-app
```

#### **Khi KHÔNG thay đổi code backend (chỉ restart):**

```powershell
cd E:\BOOKNEST_ETL
docker-compose restart
docker logs -f booknest-etl-app
```

#### **Khi lần đầu tiên chạy:**

```powershell
cd E:\BOOKNEST_ETL
mvn clean package -DskipTests
docker-compose up -d
docker logs -f booknest-etl-app
```

---

### **Option 2: Chạy Local (Không Docker)** 💻

#### **Backend:**

```powershell
cd E:\BOOKNEST_ETL
mvn clean package -DskipTests
java -jar target\booknest-etl-0.0.1-SNAPSHOT.jar
```

**Lưu ý:** Cần có MySQL và RabbitMQ chạy local hoặc cấu hình trong `application.yml`

---

### **Frontend (Luôn chạy riêng):** 🎨

```powershell
cd E:\BOOKNEST_ETL\presentation-ui
npm run dev
```

**Truy cập:** `http://localhost:3002`

---

## 🔄 Workflow Thông Thường

### **Scenario 1: Sửa Backend Code**

```powershell
# 1. Sửa code Java
# 2. Build lại
cd E:\BOOKNEST_ETL
mvn clean package -DskipTests

# 3. Rebuild Docker image
docker-compose build --no-cache

# 4. Restart containers
docker-compose down
docker-compose up -d

# 5. Check logs
docker logs -f booknest-etl-app
```

---

### **Scenario 2: Sửa Frontend Code**

```powershell
# Frontend tự động hot-reload, không cần restart
# Chỉ cần save file là đủ
```

---

### **Scenario 3: Sửa docker-compose.yml hoặc Dockerfile**

```powershell
cd E:\BOOKNEST_ETL
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

---

## 🛠️ Các Lệnh Hữu Ích

### **Kiểm tra trạng thái containers:**

```powershell
docker-compose ps
```

**Output mong đợi:**

```
NAME                   STATUS    PORTS
booknest-etl-app       Up        0.0.0.0:8080->8080/tcp
booknest-rabbitmq      Up        0.0.0.0:5673->5672/tcp, 0.0.0.0:15673->15672/tcp
mysql-source           Up        0.0.0.0:3310->3306/tcp
```

---

### **Xem logs của từng service:**

```powershell
# Backend app
docker logs -f booknest-etl-app

# RabbitMQ
docker logs -f booknest-rabbitmq

# MySQL
docker logs -f mysql-source

# Xem logs của tất cả
docker-compose logs -f
```

---

### **Restart từng service:**

```powershell
# Restart app
docker-compose restart booknest-etl-app

# Restart RabbitMQ
docker-compose restart booknest-rabbitmq

# Restart MySQL
docker-compose restart mysql-source
```

---

### **Stop tất cả services:**

```powershell
docker-compose stop
```

---

### **Stop và xóa containers + volumes:**

```powershell
docker-compose down -v
```

**⚠️ Lưu ý:** Sẽ xóa tất cả data trong MySQL!

---

### **Vào shell của container:**

```powershell
# Vào app container
docker exec -it booknest-etl-app sh

# Vào MySQL container
docker exec -it mysql-source mysql -uroot -proot

# Check database
docker exec -it mysql-source mysql -uroot -proot -e "SHOW DATABASES;"
```

---

### **Check port đang sử dụng:**

```powershell
# Check port 8080 (backend)
netstat -ano | findstr :8080

# Check port 3002 (frontend)
netstat -ano | findstr :3002

# Check port 5672 (RabbitMQ)
netstat -ano | findstr :5672

# Check port 3306 (MySQL)
netstat -ano | findstr :3306
```

---

## 🐛 Troubleshooting

### **Problem 1: Port already in use**

**Error:** `Bind for 0.0.0.0:8080 failed: port is already allocated`

**Solution:**

```powershell
# Tìm process đang dùng port
netstat -ano | findstr :8080

# Kill process (thay <PID> bằng số PID tìm được)
taskkill /PID <PID> /F

# Hoặc đổi port trong docker-compose.yml
ports:
  - "8081:8080"  # Đổi từ 8080 sang 8081
```

---

### **Problem 2: Container khởi động nhưng crash ngay**

**Check logs:**

```powershell
docker logs booknest-etl-app
```

**Thường gặp:**

- Không connect được MySQL: Check `mysql-source` container có running không
- Không connect được RabbitMQ: Check `booknest-rabbitmq` container có running không

**Solution:**

```powershell
# Restart MySQL và RabbitMQ trước
docker-compose restart mysql-source booknest-rabbitmq

# Đợi 10 giây để chúng ready
Start-Sleep -Seconds 10

# Restart app
docker-compose restart booknest-etl-app
```

---

### **Problem 3: Frontend không connect được backend**

**Error:** `Network Error` hoặc `ERR_CONNECTION_REFUSED`

**Check:**

```powershell
# 1. Check backend có chạy không
curl http://localhost:8080/actuator/health

# 2. Check logs
docker logs booknest-etl-app

# 3. Check vite.config.js có proxy đúng không
# Phải có:
proxy: {
  "/api": {
    target: "http://localhost:8080",
    changeOrigin: true,
  },
}
```

---

### **Problem 4: MySQL connection refused**

**Error:** `Connection refused` hoặc `Unknown database`

**Solution:**

```powershell
# 1. Check MySQL container
docker logs mysql-source

# 2. Vào MySQL và check databases
docker exec -it mysql-source mysql -uroot -proot -e "SHOW DATABASES;"

# 3. Nếu không có source_db và staging_db, chạy init script
docker exec -i mysql-source mysql -uroot -proot < E:\BOOKNEST_ETL\scripts\init-mysql.sql

# Hoặc restart với clean volumes
docker-compose down -v
docker-compose up -d
```

---

### **Problem 5: Build Maven thất bại**

**Error:** `BUILD FAILURE`

**Solution:**

```powershell
# 1. Clean cache
mvn clean

# 2. Skip tests
mvn clean package -DskipTests

# 3. Nếu vẫn lỗi, xóa .m2 cache
Remove-Item -Recurse -Force $HOME\.m2\repository\com\booknest

# 4. Build lại
mvn clean package -DskipTests
```

---

## 📊 Health Check Endpoints

### **Backend Health:**

```powershell
curl http://localhost:8080/actuator/health
```

**Expected:**

```json
{ "status": "UP" }
```

---

### **RabbitMQ Management UI:**

```
http://localhost:15673
Username: guest
Password: guest
```

---

### **MySQL Check:**

```powershell
docker exec -it mysql-source mysql -uroot -proot -e "SELECT 1"
```

---

## 🎯 Quick Start (Demo Day)

### **Script đơn giản nhất cho ngày demo:**

```powershell
# 1. Start tất cả services
cd E:\BOOKNEST_ETL
docker-compose up -d

# 2. Đợi services ready (30 giây)
Start-Sleep -Seconds 30

# 3. Check logs
docker logs --tail 50 booknest-etl-app

# 4. Start frontend (terminal mới)
cd E:\BOOKNEST_ETL\presentation-ui
npm run dev

# 5. Mở browser: http://localhost:3002
```

---

## 🧪 Verify Everything Works

### **Checklist:**

```powershell
# 1. Check Docker containers
docker-compose ps
# → Tất cả phải "Up"

# 2. Check backend health
curl http://localhost:8080/actuator/health
# → {"status":"UP"}

# 3. Check RabbitMQ
curl http://localhost:15673
# → RabbitMQ Management UI

# 4. Check MySQL
docker exec -it mysql-source mysql -uroot -proot -e "SHOW DATABASES;"
# → Phải có source_db và staging_db

# 5. Check frontend
# → Mở http://localhost:3002
# → Thấy giao diện upload

# 6. Test upload
# → Upload customers_source.csv
# → Thấy kết quả với comparison
```

---

## 📝 Notes

### **Khi nào cần `--no-cache`:**

- ✅ Có thay đổi code backend
- ✅ Có thay đổi dependencies (pom.xml)
- ✅ Có thay đổi Dockerfile
- ❌ Chỉ restart services

### **Khi nào cần `mvn clean package`:**

- ✅ Có thay đổi code Java
- ✅ Có thay đổi pom.xml
- ✅ Có thay đổi resources (application.yml, SQL scripts)
- ❌ Chỉ thay đổi docker-compose.yml

### **Khi nào cần `docker-compose down`:**

- ✅ Có thay đổi docker-compose.yml
- ✅ Cần xóa và tạo lại containers
- ✅ Có vấn đề về network hoặc volumes
- ❌ Chỉ cần restart

---

## 🎉 Ready for Demo!

**Workflow cuối cùng trước khi demo:**

```powershell
# 1. Test build
cd E:\BOOKNEST_ETL
mvn clean package -DskipTests

# 2. Start services
docker-compose up -d

# 3. Wait
Start-Sleep -Seconds 30

# 4. Verify
curl http://localhost:8080/actuator/health
docker logs --tail 20 booknest-etl-app

# 5. Start frontend
cd E:\BOOKNEST_ETL\presentation-ui
npm run dev

# 6. Test upload với customers_source.csv
# 7. Kiểm tra hiển thị Original vs Transformed
# 8. Test sửa lỗi
# 9. Test export CSV
# 10. Test load to database

# ✅ SẴN SÀNG DEMO!
```
