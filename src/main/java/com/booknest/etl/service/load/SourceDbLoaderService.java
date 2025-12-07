package com.booknest.etl.service.load;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Qualifier;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Service
@Slf4j
public class SourceDbLoaderService {

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate stagingJdbcTemplate;

    public SourceDbLoaderService(
            @Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
            @Qualifier("stagingJdbcTemplate") JdbcTemplate stagingJdbcTemplate) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.stagingJdbcTemplate = stagingJdbcTemplate;
    }


    @Transactional
    public int loadCustomersToSource() {
        log.info("Loading validated customers from staging_db to source_db...");

        String selectSql = """
                SELECT customer_key, full_name, email, phone, roles, status
                FROM staging_db.stg_customers
                WHERE quality_status = 'VALIDATED'
                """;

        List<CustomerStaging> customers = stagingJdbcTemplate.query(selectSql, (rs, rowNum) -> {
            CustomerStaging c = new CustomerStaging();
            c.customerKey = rs.getString("customer_key");
            c.fullName = rs.getString("full_name");
            c.email = rs.getString("email");
            c.phone = rs.getString("phone");
            c.roles = rs.getString("roles");
            c.status = rs.getString("status");
            return c;
        });

        int loaded = 0;
        for (CustomerStaging customer : customers) {
            try {
                String upsertUserSql = """
                        INSERT INTO source_db.nguoi_dung (ho_ten, email, sdt, mat_khau_hash, trang_thai, ngay_tao)
                        VALUES (?, ?, ?, 'default_hash', ?, NOW())
                        ON DUPLICATE KEY UPDATE
                            ho_ten = VALUES(ho_ten),
                            sdt = VALUES(sdt),
                            trang_thai = VALUES(trang_thai),
                            ngay_cap_nhat = NOW()
                        """;

                sourceJdbcTemplate.update(upsertUserSql,
                        customer.fullName,
                        customer.email,
                        customer.phone,
                        mapStatus(customer.status)
                );

                Integer userId = sourceJdbcTemplate.queryForObject(
                        "SELECT ma_nguoi_dung FROM source_db.nguoi_dung WHERE email = ?",
                        Integer.class,
                        customer.email
                );

                if (customer.roles != null && !customer.roles.isEmpty() && userId != null) {
                    String[] roleNames = customer.roles.split("[,|]");
                    for (String roleName : roleNames) {
                        String trimmedRole = roleName.trim();
                        if (!trimmedRole.isEmpty()) {
                            Integer roleId = getOrCreateRole(trimmedRole);
                            if (roleId != null) {
                                sourceJdbcTemplate.update(
                                        "INSERT IGNORE INTO source_db.nguoi_dung_vai_tro (ma_nguoi_dung, ma_vai_tro) VALUES (?, ?)",
                                        userId, roleId
                                );
                            }
                        }
                    }
                }

                loaded++;
            } catch (Exception e) {
                log.error("Error loading customer {}: {}", customer.customerKey, e.getMessage());
            }
        }

        log.info("Loaded {} customers to source_db", loaded);
        return loaded;
    }

    @Transactional
    public int loadBooksToSource() {
        log.info("Loading validated books from staging_db to source_db...");

        String selectSql = """
                SELECT book_key, title, authors, categories, description, price, free_flag, released_at
                FROM staging_db.stg_books
                WHERE quality_status = 'VALIDATED'
                """;

        List<BookStaging> books = stagingJdbcTemplate.query(selectSql, (rs, rowNum) -> {
            BookStaging b = new BookStaging();
            b.bookKey = rs.getString("book_key");
            b.title = rs.getString("title");
            b.authors = rs.getString("authors");
            b.categories = rs.getString("categories");
            b.description = rs.getString("description");
            b.price = rs.getBigDecimal("price");
            b.freeFlag = rs.getBoolean("free_flag");
            b.releasedAt = rs.getDate("released_at");
            return b;
        });

        int loaded = 0;
        for (BookStaging book : books) {
            try {
                String upsertBookSql = """
                        INSERT INTO source_db.sach (ten_sach, mo_ta, gia_ban, mien_phi, ngay_phat_hanh, trang_thai)
                        VALUES (?, ?, ?, ?, ?, 'HIEU_LUC')
                        ON DUPLICATE KEY UPDATE
                            ten_sach = VALUES(ten_sach),
                            mo_ta = VALUES(mo_ta),
                            gia_ban = VALUES(gia_ban),
                            mien_phi = VALUES(mien_phi),
                            ngay_phat_hanh = VALUES(ngay_phat_hanh)
                        """;

                sourceJdbcTemplate.update(upsertBookSql,
                        book.title,
                        book.description,
                        book.price,
                        book.freeFlag,
                        book.releasedAt
                );

                Integer bookId = sourceJdbcTemplate.queryForObject(
                        "SELECT ma_sach FROM source_db.sach WHERE ten_sach = ? ORDER BY ma_sach DESC LIMIT 1",
                        Integer.class,
                        book.title
                );

                if (bookId != null) {
                    if (book.authors != null && !book.authors.isEmpty()) {
                        String[] authorNames = book.authors.split("[,|]");
                        for (String authorName : authorNames) {
                            String trimmedAuthor = authorName.trim();
                            if (!trimmedAuthor.isEmpty()) {
                                Integer authorId = getOrCreateAuthor(trimmedAuthor);
                                if (authorId != null) {
                                    sourceJdbcTemplate.update(
                                            "INSERT IGNORE INTO source_db.sach_tac_gia (ma_sach, ma_tac_gia) VALUES (?, ?)",
                                            bookId, authorId
                                    );
                                }
                            }
                        }
                    }

                    if (book.categories != null && !book.categories.isEmpty()) {
                        String[] categoryNames = book.categories.split("[,|]");
                        for (String categoryName : categoryNames) {
                            String trimmedCategory = categoryName.trim();
                            if (!trimmedCategory.isEmpty()) {
                                Integer categoryId = getOrCreateCategory(trimmedCategory);
                                if (categoryId != null) {
                                    sourceJdbcTemplate.update(
                                            "INSERT IGNORE INTO source_db.sach_the_loai (ma_sach, ma_the_loai) VALUES (?, ?)",
                                            bookId, categoryId
                                    );
                                }
                            }
                        }
                    }
                }

                loaded++;
            } catch (Exception e) {
                log.error("Error loading book {}: {}", book.bookKey, e.getMessage());
            }
        }

        log.info("Loaded {} books to source_db", loaded);
        return loaded;
    }

    @Transactional
    public int loadOrdersToSource() {
        log.info("Loading validated orders from staging_db to source_db...");

        String selectSql = """
                SELECT order_key, customer_key, status, payment_method, subtotal, discount, shipping_fee, total_amount,
                       receiver_name, receiver_phone, receiver_address, payment_ref, order_date
                FROM staging_db.stg_orders
                WHERE quality_status = 'VALIDATED'
                """;

        List<OrderStaging> orders = stagingJdbcTemplate.query(selectSql, (rs, rowNum) -> {
            OrderStaging o = new OrderStaging();
            o.orderKey = rs.getString("order_key");
            o.customerKey = rs.getString("customer_key");
            o.status = rs.getString("status");
            o.paymentMethod = rs.getString("payment_method");
            o.subtotal = rs.getBigDecimal("subtotal");
            o.discount = rs.getBigDecimal("discount");
            o.shippingFee = rs.getBigDecimal("shipping_fee");
            o.totalAmount = rs.getBigDecimal("total_amount");
            o.receiverName = rs.getString("receiver_name");
            o.receiverPhone = rs.getString("receiver_phone");
            o.receiverAddress = rs.getString("receiver_address");
            o.paymentRef = rs.getString("payment_ref");
            o.orderDate = rs.getTimestamp("order_date");
            return o;
        });

        int loaded = 0;
        int skipped = 0;
        for (OrderStaging order : orders) {
            try {
                Integer userId = null;
                try {
                    userId = sourceJdbcTemplate.queryForObject(
                            "SELECT ma_nguoi_dung FROM source_db.nguoi_dung WHERE email = ? LIMIT 1",
                            Integer.class,
                            order.customerKey
                    );
                } catch (Exception e) {
                }

                if (userId == null) {
                    log.warn("⚠️ Order {} skipped: customer '{}' not found. Upload customers_source.csv first!", 
                            order.orderKey, order.customerKey);
                    skipped++;
                    continue;
                }

                String upsertOrderSql = """
                        INSERT INTO source_db.don_hang (ma_nguoi_dung, external_order_id, trang_thai, phuong_thuc_thanh_toan,
                                                        tien_hang, giam_gia, phi_vc, tong_tien,
                                                        ten_nguoi_nhan, sdt_nguoi_nhan, dia_chi_nhan,
                                                        ma_tham_chieu_thanh_toan, ngay_tao)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            trang_thai = VALUES(trang_thai),
                            tien_hang = VALUES(tien_hang),
                            giam_gia = VALUES(giam_gia),
                            phi_vc = VALUES(phi_vc),
                            tong_tien = VALUES(tong_tien),
                            ngay_cap_nhat = NOW()
                        """;

                sourceJdbcTemplate.update(upsertOrderSql,
                        userId,
                        order.orderKey,
                        order.status,
                        order.paymentMethod,
                        order.subtotal,
                        order.discount,
                        order.shippingFee,
                        order.totalAmount,
                        order.receiverName,
                        order.receiverPhone,
                        order.receiverAddress,
                        order.paymentRef,
                        order.orderDate
                );

                loaded++;
            } catch (Exception e) {
                log.error("Error loading order {}: {}", order.orderKey, e.getMessage());
            }
        }

        if (skipped > 0) {
            log.warn("{} orders skipped due to missing customers. Upload customers first!", skipped);
        }
        log.info("Loaded {} orders to source_db ({} skipped)", loaded, skipped);
        return loaded;
    }

    private Integer getOrCreateRole(String roleName) {
        try {
            return sourceJdbcTemplate.queryForObject(
                    "SELECT ma_vai_tro FROM source_db.vai_tro WHERE ten_vai_tro = ?",
                    Integer.class,
                    roleName
            );
        } catch (Exception e) {
            sourceJdbcTemplate.update("INSERT INTO source_db.vai_tro (ten_vai_tro) VALUES (?)", roleName);
            return sourceJdbcTemplate.queryForObject(
                    "SELECT ma_vai_tro FROM source_db.vai_tro WHERE ten_vai_tro = ?",
                    Integer.class,
                    roleName
            );
        }
    }

    private Integer getOrCreateAuthor(String authorName) {
        try {
            return sourceJdbcTemplate.queryForObject(
                    "SELECT ma_tac_gia FROM source_db.tac_gia WHERE ten_tac_gia = ?",
                    Integer.class,
                    authorName
            );
        } catch (Exception e) {
            sourceJdbcTemplate.update("INSERT INTO source_db.tac_gia (ten_tac_gia) VALUES (?)", authorName);
            return sourceJdbcTemplate.queryForObject(
                    "SELECT ma_tac_gia FROM source_db.tac_gia WHERE ten_tac_gia = ?",
                    Integer.class,
                    authorName
            );
        }
    }

    private Integer getOrCreateCategory(String categoryName) {
        try {
            return sourceJdbcTemplate.queryForObject(
                    "SELECT ma_the_loai FROM source_db.the_loai WHERE ten_the_loai = ?",
                    Integer.class,
                    categoryName
            );
        } catch (Exception e) {
            sourceJdbcTemplate.update("INSERT INTO source_db.the_loai (ten_the_loai) VALUES (?)", categoryName);
            return sourceJdbcTemplate.queryForObject(
                    "SELECT ma_the_loai FROM source_db.the_loai WHERE ten_the_loai = ?",
                    Integer.class,
                    categoryName
            );
        }
    }

    private String mapStatus(String status) {
        if (status == null) return "HOAT_DONG";
        return status.toUpperCase().equals("HOAT_DONG") || status.toUpperCase().equals("ACTIVE") 
                ? "HOAT_DONG" : "KHOA";
    }

    private static class CustomerStaging {
        String customerKey;
        String fullName;
        String email;
        String phone;
        String roles;
        String status;
    }

    private static class BookStaging {
        String bookKey;
        String title;
        String authors;
        String categories;
        String description;
        java.math.BigDecimal price;
        Boolean freeFlag;
        java.sql.Date releasedAt;
    }

    private static class OrderStaging {
        String orderKey;
        String customerKey;
        String status;
        String paymentMethod;
        java.math.BigDecimal subtotal;
        java.math.BigDecimal discount;
        java.math.BigDecimal shippingFee;
        java.math.BigDecimal totalAmount;
        String receiverName;
        String receiverPhone;
        String receiverAddress;
        String paymentRef;
        java.sql.Timestamp orderDate;
    }


    public int loadOrderItemsToSource() {
        log.info("Loading validated order items from staging_db to source_db...");

        String selectSql = """
                SELECT order_key, book_key, quantity, unit_price
                FROM staging_db.stg_order_items
                WHERE quality_status = 'VALIDATED'
                """;

        List<Map<String, Object>> items = stagingJdbcTemplate.queryForList(selectSql);

        int loaded = 0;
        int skipped = 0;
        for (Map<String, Object> item : items) {
            try {
                String orderKey = (String) item.get("order_key");
                String bookKey = (String) item.get("book_key");
                Integer quantity = (Integer) item.get("quantity");
                java.math.BigDecimal unitPrice = (java.math.BigDecimal) item.get("unit_price");

                Integer orderId = null;
                try {
                    orderId = sourceJdbcTemplate.queryForObject(
                            "SELECT ma_don_hang FROM source_db.don_hang WHERE external_order_id = ? LIMIT 1",
                            Integer.class,
                            orderKey
                    );
                } catch (Exception e) {
                    log.warn("Order item skipped: order '{}' not found in source_db", orderKey);
                    skipped++;
                    continue;
                }
                Integer bookId = null;
                try {
                    bookId = sourceJdbcTemplate.queryForObject(
                            "SELECT ma_sach FROM source_db.sach WHERE ten_sach = ? LIMIT 1",
                            Integer.class,
                            bookKey
                    );
                } catch (Exception e) {
                    log.warn("Order item skipped: book '{}' not found in source_db", bookKey);
                    skipped++;
                    continue;
                }

                String upsertSql = """
                        INSERT INTO source_db.chi_tiet_don_hang (ma_don_hang, ma_sach, so_luong, don_gia)
                        VALUES (?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            so_luong = VALUES(so_luong),
                            don_gia = VALUES(don_gia)
                        """;

                sourceJdbcTemplate.update(upsertSql, orderId, bookId, quantity, unitPrice);
                loaded++;
                
            } catch (Exception e) {
                log.error("Error loading order item: {}", e.getMessage());
                skipped++;
            }
        }

        if (skipped > 0) {
            log.warn("{} order items skipped (order or book not found)", skipped);
        }
        log.info("Loaded {} order items to source_db ({} skipped)", loaded, skipped);
        return loaded;
    }

    public int loadCartsToSource() {
        log.info("Loading validated carts from staging_db to source_db...");

        String selectSql = """
                SELECT cart_key, customer_key, created_at
                FROM staging_db.stg_carts
                WHERE quality_status = 'VALIDATED'
                """;

        List<Map<String, Object>> carts = stagingJdbcTemplate.queryForList(selectSql);

        int loaded = 0;
        int skipped = 0;
        for (Map<String, Object> cart : carts) {
            try {
                String cartKey = (String) cart.get("cart_key");
                String customerKey = (String) cart.get("customer_key");
                java.sql.Timestamp createdAt = (java.sql.Timestamp) cart.get("created_at");

                Integer userId = null;
                try {
                    userId = sourceJdbcTemplate.queryForObject(
                            "SELECT ma_nguoi_dung FROM source_db.nguoi_dung WHERE email = ? LIMIT 1",
                            Integer.class,
                            customerKey
                    );
                } catch (Exception e) {
                    log.warn("Cart {} skipped: customer '{}' not found", cartKey, customerKey);
                    skipped++;
                    continue;
                }

                String upsertSql = """
                        INSERT INTO source_db.gio_hang (ma_nguoi_dung, ngay_tao)
                        VALUES (?, ?)
                        ON DUPLICATE KEY UPDATE
                            ngay_cap_nhat = NOW()
                        """;

                sourceJdbcTemplate.update(upsertSql, userId, createdAt);
                loaded++;
            } catch (Exception e) {
                log.error("Error loading cart: {}", e.getMessage());
            }
        }

        log.info("Loaded {} carts to source_db ({} skipped)", loaded, skipped);
        return loaded;
    }

    public int loadInvoicesToSource() {
        log.info("Loading validated invoices from staging_db to source_db...");

        String selectSql = """
                SELECT invoice_key, order_key, amount, status, issued_at, due_at
                FROM staging_db.stg_invoices
                WHERE quality_status = 'VALIDATED'
                """;

        List<Map<String, Object>> invoices = stagingJdbcTemplate.queryForList(selectSql);

        int loaded = 0;
        int skipped = 0;
        for (Map<String, Object> invoice : invoices) {
            try {
                String invoiceKey = (String) invoice.get("invoice_key");
                String orderKey = (String) invoice.get("order_key");
                java.math.BigDecimal amount = (java.math.BigDecimal) invoice.get("amount");
                if (amount == null) {
                    amount = java.math.BigDecimal.ZERO;
                }
                String status = (String) invoice.get("status");
                java.sql.Timestamp issuedAt = (java.sql.Timestamp) invoice.get("issued_at");
                java.sql.Timestamp dueAt = (java.sql.Timestamp) invoice.get("due_at");

                Integer orderId = null;
                try {
                    orderId = sourceJdbcTemplate.queryForObject(
                            "SELECT ma_don_hang FROM source_db.don_hang WHERE external_order_id = ? LIMIT 1",
                            Integer.class,
                            orderKey
                    );
                } catch (Exception e) {
                }

                if (orderId == null) {
                    log.warn("Invoice {} skipped: order '{}' not found in source_db", invoiceKey, orderKey);
                    skipped++;
                    continue;
                }

                String upsertSql = """
                        INSERT INTO source_db.hoa_don (ma_don_hang, so_tien, trang_thai_thanh_toan, ngay_tao)
                        VALUES (?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            so_tien = VALUES(so_tien),
                            trang_thai_thanh_toan = VALUES(trang_thai_thanh_toan)
                        """;

                java.sql.Timestamp createdAt = issuedAt != null ? issuedAt : new java.sql.Timestamp(System.currentTimeMillis());
                String paymentStatus = mapInvoicePaymentStatus(status);

                sourceJdbcTemplate.update(upsertSql, orderId, amount, paymentStatus, createdAt);
                loaded++;
            } catch (Exception e) {
                log.error("Error loading invoice: {}", e.getMessage());
                skipped++;
            }
        }

        log.info("Loaded {} invoices to source_db ({} skipped)", loaded, skipped);
        return loaded;
    }

    private String mapInvoicePaymentStatus(String status) {
        if (status == null) return "CHUA_TT";
        String normalized = status.trim().toUpperCase();
        if (normalized.equals("PAID")
                || normalized.equals("DA_THANH_TOAN")
                || normalized.equals("REFUNDED")
                || normalized.equals("DA_HOAN_TIEN")) {
            return "DA_TT";
        }
        return "CHUA_TT";
    }

}
