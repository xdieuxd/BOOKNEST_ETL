package com.booknest.etl.repository.book;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.booknest.etl.dto.BookRawMessage;

@Repository
public class BookJdbcRepository {

    private static final String BASE_SELECT = """
            SELECT s.ma_sach,
                   s.ten_sach,
                   s.mo_ta,
                   s.gia_ban,
                   s.mien_phi,
                   s.ngay_phat_hanh,
                   s.trang_thai,
                   s.diem_trung_binh,
                   s.luot_mua,
                   GROUP_CONCAT(DISTINCT tg.ten_tac_gia SEPARATOR '||') AS authors,
                   GROUP_CONCAT(DISTINCT tl.ten_the_loai SEPARATOR '||') AS categories
            FROM sach s
            LEFT JOIN sach_tac_gia stg ON stg.ma_sach = s.ma_sach
            LEFT JOIN tac_gia tg ON tg.ma_tac_gia = stg.ma_tac_gia
            LEFT JOIN sach_the_loai stl ON stl.ma_sach = s.ma_sach
            LEFT JOIN the_loai tl ON tl.ma_the_loai = stl.ma_the_loai
            """;

    private final JdbcTemplate jdbcTemplate;

    public BookJdbcRepository(JdbcTemplate sourceJdbcTemplate) {
        this.jdbcTemplate = sourceJdbcTemplate;
    }

    public List<BookRawMessage> findAllBooks() {
        String sql = BASE_SELECT + " GROUP BY s.ma_sach";
        return jdbcTemplate.query(sql, this::mapRowToBook);
    }

    public Optional<BookRawMessage> findById(String id) {
        String sql = BASE_SELECT + " WHERE s.ma_sach = ? GROUP BY s.ma_sach";
        List<BookRawMessage> list = jdbcTemplate.query(sql, this::mapRowToBook, id);
        return list.stream().findFirst();
    }

    private BookRawMessage mapRowToBook(ResultSet rs, int rowNum) throws SQLException {
        return BookRawMessage.builder()
                .source("source_db")
                .bookId(String.valueOf(rs.getInt("ma_sach")))
                .title(rs.getString("ten_sach"))
                .description(rs.getString("mo_ta"))
                .price(Optional.ofNullable(rs.getBigDecimal("gia_ban")).orElse(null))
                .free(rs.getBoolean("mien_phi"))
                .releasedAt(rs.getDate("ngay_phat_hanh") != null ? rs.getDate("ngay_phat_hanh").toLocalDate() : null)
                .status(rs.getString("trang_thai"))
                .averageRating(rs.getBigDecimal("diem_trung_binh"))
                .totalOrders(rs.getObject("luot_mua") != null ? rs.getInt("luot_mua") : null)
                .authors(split(rs.getString("authors")))
                .categories(split(rs.getString("categories")))
                .extractedAt(OffsetDateTime.now())
                .build();
    }

    private List<String> split(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        return Arrays.stream(raw.split("\\|\\|"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}
