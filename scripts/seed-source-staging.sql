
DROP DATABASE IF EXISTS source_db;
CREATE DATABASE source_db CHARACTER SET utf8mb4 COLLATE utf8mb4_vi_0900_ai_ci;

DROP DATABASE IF EXISTS staging_db;
CREATE DATABASE staging_db CHARACTER SET utf8mb4 COLLATE utf8mb4_vi_0900_ai_ci;

USE source_db;

-- Catalog tables
CREATE TABLE tac_gia (
  ma_tac_gia INT AUTO_INCREMENT PRIMARY KEY,
  ten_tac_gia VARCHAR(150) CHARACTER SET utf8mb4 NOT NULL
);

CREATE TABLE the_loai (
  ma_the_loai INT AUTO_INCREMENT PRIMARY KEY,
  ten_the_loai VARCHAR(150) CHARACTER SET utf8mb4 NOT NULL UNIQUE
);

CREATE TABLE sach (
  ma_sach INT AUTO_INCREMENT PRIMARY KEY,
  ten_sach VARCHAR(300) CHARACTER SET utf8mb4 NOT NULL,
  mo_ta LONGTEXT CHARACTER SET utf8mb4,
  gia_ban DECIMAL(12,2) NOT NULL DEFAULT 0,
  mien_phi BOOLEAN NOT NULL DEFAULT FALSE,
  ngay_phat_hanh DATE,
  trang_thai ENUM('AN','HIEU_LUC') DEFAULT 'HIEU_LUC',
  diem_trung_binh DECIMAL(3,2) DEFAULT 0,
  luot_mua INT DEFAULT 0
);

CREATE TABLE sach_tac_gia (
  ma_sach INT NOT NULL,
  ma_tac_gia INT NOT NULL,
  PRIMARY KEY (ma_sach, ma_tac_gia),
  FOREIGN KEY (ma_sach) REFERENCES sach(ma_sach),
  FOREIGN KEY (ma_tac_gia) REFERENCES tac_gia(ma_tac_gia)
);

CREATE TABLE sach_the_loai (
  ma_sach INT NOT NULL,
  ma_the_loai INT NOT NULL,
  PRIMARY KEY (ma_sach, ma_the_loai),
  FOREIGN KEY (ma_sach) REFERENCES sach(ma_sach),
  FOREIGN KEY (ma_the_loai) REFERENCES the_loai(ma_the_loai)
);

CREATE TABLE anh_sach (
  ma_anh INT AUTO_INCREMENT PRIMARY KEY,
  ma_sach INT NOT NULL,
  url VARCHAR(500) NOT NULL,
  thu_tu INT DEFAULT 0,
  FOREIGN KEY (ma_sach) REFERENCES sach(ma_sach)
);

CREATE TABLE tai_san_tap_tin (
  ma_tai_san INT AUTO_INCREMENT PRIMARY KEY,
  ma_sach INT,
  loai_tap_tin ENUM('ANH','PDF','EPUB','PREVIEW') NOT NULL,
  url VARCHAR(500) NOT NULL,
  kich_thuoc INT,
  mime_type VARCHAR(100),
  FOREIGN KEY (ma_sach) REFERENCES sach(ma_sach)
);

-- User & role
CREATE TABLE vai_tro (
  ma_vai_tro INT AUTO_INCREMENT PRIMARY KEY,
  ten_vai_tro VARCHAR(30) CHARACTER SET utf8mb4 NOT NULL UNIQUE
);

CREATE TABLE nguoi_dung (
  ma_nguoi_dung INT AUTO_INCREMENT PRIMARY KEY,
  ho_ten VARCHAR(150) CHARACTER SET utf8mb4 NOT NULL,
  email VARCHAR(150) NOT NULL UNIQUE,
  sdt VARCHAR(20),
  mat_khau_hash VARCHAR(200) NOT NULL,
  trang_thai ENUM('HOAT_DONG','KHOA') DEFAULT 'HOAT_DONG',
  ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ngay_cap_nhat TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE nguoi_dung_vai_tro (
  ma_nguoi_dung INT NOT NULL,
  ma_vai_tro INT NOT NULL,
  PRIMARY KEY (ma_nguoi_dung, ma_vai_tro),
  FOREIGN KEY (ma_nguoi_dung) REFERENCES nguoi_dung(ma_nguoi_dung),
  FOREIGN KEY (ma_vai_tro) REFERENCES vai_tro(ma_vai_tro)
);

-- Cart & orders
CREATE TABLE gio_hang (
  ma_gio_hang INT AUTO_INCREMENT PRIMARY KEY,
  ma_nguoi_dung INT NOT NULL,
  ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_gio_hang_nguoi_dung FOREIGN KEY (ma_nguoi_dung) REFERENCES nguoi_dung(ma_nguoi_dung)
);

CREATE TABLE chi_tiet_gio_hang (
  ma_gio_hang INT NOT NULL,
  ma_sach INT NOT NULL,
  so_luong INT NOT NULL CHECK (so_luong > 0),
  gia_ban DECIMAL(12,2) NOT NULL,
  PRIMARY KEY (ma_gio_hang, ma_sach),
  CONSTRAINT fk_ctgh_gio_hang FOREIGN KEY (ma_gio_hang) REFERENCES gio_hang(ma_gio_hang) ON DELETE CASCADE,
  CONSTRAINT fk_ctgh_sach FOREIGN KEY (ma_sach) REFERENCES sach(ma_sach),
  INDEX idx_ctgh_sach (ma_sach)
);

CREATE TABLE don_hang (
  ma_don_hang INT AUTO_INCREMENT PRIMARY KEY,
  ma_nguoi_dung INT NOT NULL,
  trang_thai ENUM('TAO_MOI','CHO_THANH_TOAN','DA_THANH_TOAN','DANG_GIAO','DA_NHAN','DA_HUY','HOAN_TIEN') NOT NULL DEFAULT 'TAO_MOI',
  phuong_thuc_thanh_toan ENUM('ONLINE','COD') NOT NULL,
  tien_hang DECIMAL(12,2) NOT NULL,
  giam_gia DECIMAL(12,2) NOT NULL DEFAULT 0,
  phi_vc DECIMAL(12,2) NOT NULL DEFAULT 0,
  tong_tien DECIMAL(12,2) NOT NULL,
  CHECK (tien_hang >= 0 AND giam_gia >= 0 AND phi_vc >= 0 AND tong_tien = tien_hang - giam_gia + phi_vc),
  ten_nguoi_nhan VARCHAR(150) CHARACTER SET utf8mb4,
  sdt_nguoi_nhan VARCHAR(20),
  dia_chi_nhan VARCHAR(300) CHARACTER SET utf8mb4,
  ma_tham_chieu_thanh_toan VARCHAR(100),
  ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ngay_cap_nhat TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_dh_tham_chieu (ma_tham_chieu_thanh_toan),
  INDEX idx_dh_user_ngay (ma_nguoi_dung, ngay_tao),
  INDEX idx_dh_trang_thai_ngay (trang_thai, ngay_tao),
  CONSTRAINT fk_don_hang_nguoi_dung FOREIGN KEY (ma_nguoi_dung) REFERENCES nguoi_dung(ma_nguoi_dung)
);

CREATE TABLE chi_tiet_don_hang (
  ma_don_hang INT NOT NULL,
  ma_sach INT NOT NULL,
  so_luong INT NOT NULL CHECK (so_luong > 0),
  don_gia DECIMAL(12,2) NOT NULL,
  PRIMARY KEY (ma_don_hang, ma_sach),
  CONSTRAINT fk_ctdh_don FOREIGN KEY (ma_don_hang) REFERENCES don_hang(ma_don_hang) ON DELETE CASCADE,
  CONSTRAINT fk_ctdh_sach FOREIGN KEY (ma_sach) REFERENCES sach(ma_sach),
  INDEX idx_ctdh_sach (ma_sach)
);

CREATE TABLE hoa_don (
  ma_hoa_don INT AUTO_INCREMENT PRIMARY KEY,
  ma_don_hang INT NOT NULL,
  so_tien DECIMAL(12,2) NOT NULL CHECK (so_tien >= 0),
  trang_thai_thanh_toan ENUM('CHUA_TT','DA_TT') NOT NULL DEFAULT 'CHUA_TT',
  ngay_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_hd_don FOREIGN KEY (ma_don_hang) REFERENCES don_hang(ma_don_hang) ON DELETE CASCADE,
  UNIQUE KEY uq_hd_don (ma_don_hang),
  INDEX idx_hd_ngay (ngay_tao)
);

-- Seed sample data
INSERT INTO tac_gia (ten_tac_gia) VALUES ('Nguyen Nhat Anh'), ('Haruki Murakami'), ('Nguyen Ngoc Tu');
INSERT INTO the_loai (ten_the_loai) VALUES ('Thieu nhi'), ('Tieu thuyet'), ('Truyen ngan');

INSERT INTO sach (ten_sach, mo_ta, gia_ban, ngay_phat_hanh, trang_thai, diem_trung_binh, luot_mua)
VALUES ('Mat Biec', 'Tieu thuyet ve tinh yeu tuoi tre', 120000, '2010-05-01', 'HIEU_LUC', 4.6, 1200),
       ('Kafka ben bo bien', 'Tieu thuyet huyen ao', 180000, '2002-09-12', 'HIEU_LUC', 4.8, 900),
       ('Canh dong bat tan', 'Truyen ngan noi tieng', 90000, '2005-03-18', 'HIEU_LUC', 4.5, 700);

INSERT INTO sach_tac_gia VALUES (1,1),(2,2),(3,3);
INSERT INTO sach_the_loai VALUES (1,1),(2,2),(3,3);

INSERT INTO vai_tro (ten_vai_tro) VALUES ('ADMIN'), ('NHAN_VIEN'), ('THANH_VIEN');
INSERT INTO nguoi_dung (ho_ten,email,sdt,mat_khau_hash,trang_thai)
VALUES ('Tran Minh','minh@example.com','0909123123','hash1','HOAT_DONG'),
       ('Le Hoa','hoa@example.com','0909888777','hash2','HOAT_DONG'),
       ('Nguyen Binh','binh@example.com','0909666777','hash3','HOAT_DONG');

INSERT INTO nguoi_dung_vai_tro VALUES (1,1),(2,3),(3,3);

INSERT INTO gio_hang (ma_nguoi_dung) VALUES (2),(3);
INSERT INTO chi_tiet_gio_hang VALUES (1,1,1,120000),(1,2,1,180000),(2,3,2,90000);

INSERT INTO don_hang (ma_nguoi_dung,trang_thai,phuong_thuc_thanh_toan,
                      tien_hang,giam_gia,phi_vc,tong_tien,
                      ten_nguoi_nhan,sdt_nguoi_nhan,dia_chi_nhan,ma_tham_chieu_thanh_toan)
VALUES (2,'DA_THANH_TOAN','ONLINE',300000,20000,30000,310000,'Le Hoa','0909888777','HCM','TXN-001'),
       (3,'CHO_THANH_TOAN','COD',180000,0,25000,205000,'Nguyen Binh','0909666777','Ha Noi','TXN-002');

INSERT INTO chi_tiet_don_hang VALUES (1,1,1,120000),(1,2,1,180000),(2,3,2,90000);
INSERT INTO hoa_don (ma_don_hang,so_tien,trang_thai_thanh_toan)
VALUES (1,310000,'DA_TT'),(2,205000,'CHUA_TT');

USE staging_db;

CREATE TABLE dq_result (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  entity_type ENUM('BOOK','ORDER','CUSTOMER') NOT NULL,
  entity_key VARCHAR(100) NOT NULL,
  status ENUM('PASSED','FAILED','FIXED') NOT NULL DEFAULT 'PASSED',
  errors JSON NULL,
  checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_dq_entity (entity_type, entity_key)
);

CREATE TABLE stg_books (
  book_key VARCHAR(50) PRIMARY KEY,
  title VARCHAR(300) NOT NULL,
  authors VARCHAR(500) NOT NULL,
  categories VARCHAR(500) NOT NULL,
  description TEXT,
  price DECIMAL(12,2) NOT NULL,
  free_flag BOOLEAN NOT NULL DEFAULT FALSE,
  released_at DATE,
  avg_rating DECIMAL(3,2),
  total_orders INT DEFAULT 0,
  quality_status ENUM('RAW','VALIDATED','REJECTED') DEFAULT 'RAW',
  quality_errors TEXT,
  source VARCHAR(50) NOT NULL,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_customers (
  customer_key VARCHAR(50) PRIMARY KEY,
  full_name VARCHAR(150) NOT NULL,
  email VARCHAR(150) NOT NULL,
  phone VARCHAR(20),
  roles VARCHAR(100),
  status ENUM('ACTIVE','LOCKED') DEFAULT 'ACTIVE',
  quality_status ENUM('RAW','VALIDATED','REJECTED') DEFAULT 'RAW',
  quality_errors TEXT,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_orders (
  order_key VARCHAR(50) PRIMARY KEY,
  customer_key VARCHAR(50) NOT NULL,
  status ENUM('TAO_MOI','CHO_THANH_TOAN','DA_THANH_TOAN','DANG_GIAO','DA_NHAN','DA_HUY','HOAN_TIEN') NOT NULL,
  payment_method ENUM('ONLINE','COD') NOT NULL,
  subtotal DECIMAL(12,2) NOT NULL,
  discount DECIMAL(12,2) NOT NULL,
  shipping_fee DECIMAL(12,2) NOT NULL,
  total_amount DECIMAL(12,2) NOT NULL,
  payment_ref VARCHAR(100),
  receiver_name VARCHAR(150),
  receiver_phone VARCHAR(20),
  receiver_address VARCHAR(300),
  order_date DATETIME,
  updated_at DATETIME,
  quality_status ENUM('RAW','VALIDATED','REJECTED') DEFAULT 'RAW',
  quality_errors TEXT,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_order_customer (customer_key, order_date)
);

CREATE TABLE stg_order_items (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_key VARCHAR(50) NOT NULL,
  book_key VARCHAR(50) NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  line_amount DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
  quality_status ENUM('RAW','VALIDATED','REJECTED') DEFAULT 'RAW',
  quality_errors TEXT,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_items_order (order_key),
  INDEX idx_items_book (book_key)
);

CREATE TABLE stg_carts (
  cart_key VARCHAR(50) PRIMARY KEY,
  customer_key VARCHAR(50) NOT NULL,
  created_at TIMESTAMP NULL,
  quality_status ENUM('RAW','VALIDATED','REJECTED') DEFAULT 'RAW',
  quality_errors TEXT,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_cart_items (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  cart_key VARCHAR(50) NOT NULL,
  book_key VARCHAR(50) NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(12,2),
  quality_status ENUM('RAW','VALIDATED','REJECTED') DEFAULT 'RAW',
  quality_errors TEXT,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_cart_items_cart (cart_key)
);

CREATE TABLE stg_invoices (
  invoice_key VARCHAR(50) PRIMARY KEY,
  order_key VARCHAR(50) NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  status ENUM('CHUA_TT','DA_TT') NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  quality_status ENUM('RAW','VALIDATED','REJECTED') DEFAULT 'RAW',
  quality_errors TEXT,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE etl_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  job_name VARCHAR(100) NOT NULL,
  stage ENUM('EXTRACT','TRANSFORM','LOAD') NOT NULL,
  status ENUM('STARTED','SUCCESS','FAILED') NOT NULL,
  message TEXT,
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  finished_at TIMESTAMP NULL,
  source_record VARCHAR(100) NULL,
  target_record VARCHAR(100) NULL
);
