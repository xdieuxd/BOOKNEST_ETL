package com.booknest.etl.repository.user;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.booknest.etl.dto.UserRawMessage;

@Repository
public class UserJdbcRepository {

    private static final String BASE_SELECT = """
            SELECT nd.ma_nguoi_dung,
                   nd.ho_ten,
                   nd.email,
                   nd.sdt,
                   nd.trang_thai,
                   GROUP_CONCAT(vt.ten_vai_tro SEPARATOR '||') AS roles
            FROM nguoi_dung nd
            LEFT JOIN nguoi_dung_vai_tro nvt ON nvt.ma_nguoi_dung = nd.ma_nguoi_dung
            LEFT JOIN vai_tro vt ON vt.ma_vai_tro = nvt.ma_vai_tro
            """;

    private final JdbcTemplate jdbcTemplate;

    public UserJdbcRepository(JdbcTemplate sourceJdbcTemplate) {
        this.jdbcTemplate = sourceJdbcTemplate;
    }

    public List<UserRawMessage> findAllUsers() {
        String sql = BASE_SELECT + " GROUP BY nd.ma_nguoi_dung";
        return jdbcTemplate.query(sql, this::mapRowToUser);
    }

    public Optional<UserRawMessage> findById(String id) {
        String sql = BASE_SELECT + " WHERE nd.ma_nguoi_dung = ? GROUP BY nd.ma_nguoi_dung";
        return jdbcTemplate.query(sql, this::mapRowToUser, id).stream().findFirst();
    }

    private UserRawMessage mapRowToUser(ResultSet rs, int rowNum) throws SQLException {
        return UserRawMessage.builder()
                .source("source_db")
                .userId(String.valueOf(rs.getInt("ma_nguoi_dung")))
                .fullName(rs.getString("ho_ten"))
                .email(rs.getString("email"))
                .phone(rs.getString("sdt"))
                .status(rs.getString("trang_thai"))
                .roles(split(rs.getString("roles")))
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
