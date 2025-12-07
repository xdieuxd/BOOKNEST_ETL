package com.booknest.etl.service.dq;

import com.booknest.etl.dto.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.text.Normalizer;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
@Service
public class DataNormalizationService {

    private static final Map<String, String> BOOK_STATUS_MAP = Map.of(
        "ACTIVE", "HIEU_LUC",
        "INACTIVE", "AN",
        "AVAILABLE", "HIEU_LUC",
        "UNAVAILABLE", "AN",
        "HIDDEN", "AN",
        "PUBLISHED", "HIEU_LUC"
    );

    private static final Map<String, String> USER_STATUS_MAP = Map.of(
        "ACTIVE", "HOAT_DONG",
        "INACTIVE", "KHOA",
        "LOCKED", "KHOA",
        "BANNED", "KHOA",
        "ENABLED", "HOAT_DONG",
        "DISABLED", "KHOA"
    );

    private static final Map<String, String> ORDER_STATUS_MAP = Map.of(
        "PENDING", "CHO_XAC_NHAN",
        "CONFIRMED", "DA_XAC_NHAN",
        "SHIPPING", "DANG_GIAO",
        "DELIVERED", "DA_GIAO",
        "CANCELLED", "DA_HUY",
        "COMPLETED", "HOAN_THANH"
    );

    private static final Map<String, String> INVOICE_STATUS_MAP = Map.of(
        "PENDING", "CHO_THANH_TOAN",
        "PAID", "DA_THANH_TOAN",
        "CANCELLED", "DA_HUY",
        "REFUNDED", "DA_HOAN_TIEN"
    );

    public BookRawMessage normalize(BookRawMessage message) {
        if (message == null) return null;

        Map<String, String> changes = new HashMap<>();
        
        String originalStatus = message.getStatus();
        String normalizedStatus = normalizeStatus(originalStatus, BOOK_STATUS_MAP);
        if (!originalStatus.equals(normalizedStatus)) {
            changes.put("status", originalStatus + " → " + normalizedStatus);
        }

        if (!changes.isEmpty()) {
            log.debug("BookRawMessage normalized: bookId={}, changes={}", message.getBookId(), changes);
        }

        return message.toBuilder()
                .status(normalizedStatus)
                .build();
    }

    public UserRawMessage normalize(UserRawMessage message) {
        if (message == null) return null;

        Map<String, String> changes = new HashMap<>();

        // Normalize phone
        String originalPhone = message.getPhone();
        String normalizedPhone = normalizePhone(originalPhone);
        if (!originalPhone.equals(normalizedPhone)) {
            changes.put("phone", originalPhone + " → " + normalizedPhone);
        }

        // Normalize email
        String originalEmail = message.getEmail();
        String normalizedEmail = normalizeEmail(originalEmail);
        if (!originalEmail.equals(normalizedEmail)) {
            changes.put("email", originalEmail + " → " + normalizedEmail);
        }

        // Normalize status
        String originalStatus = message.getStatus();
        String normalizedStatus = normalizeStatus(originalStatus, USER_STATUS_MAP);
        if (!originalStatus.equals(normalizedStatus)) {
            changes.put("status", originalStatus + " → " + normalizedStatus);
        }

        // Normalize full name
        String originalName = message.getFullName();
        String normalizedName = normalizePersonName(originalName);
        if (!originalName.equals(normalizedName)) {
            changes.put("fullName", originalName + " → " + normalizedName);
        }

        if (!changes.isEmpty()) {
            log.info("UserRawMessage normalized: userId={}, changes={}", message.getUserId(), changes);
        }

        return message.toBuilder()
                .phone(normalizedPhone)
                .email(normalizedEmail)
                .status(normalizedStatus)
                .fullName(normalizedName)
                .build();
    }


    public OrderRawMessage normalize(OrderRawMessage message) {
        if (message == null) return null;

        Map<String, String> changes = new HashMap<>();

        // Normalize status
        String originalStatus = message.getStatus();
        String normalizedStatus = normalizeStatus(originalStatus, ORDER_STATUS_MAP);
        if (!originalStatus.equals(normalizedStatus)) {
            changes.put("status", originalStatus + " → " + normalizedStatus);
        }

        // Normalize customer name
        String originalName = message.getCustomerName();
        String normalizedName = normalizePersonName(originalName);
        if (originalName != null && !originalName.equals(normalizedName)) {
            changes.put("customerName", originalName + " → " + normalizedName);
        }

        // Normalize customer email
        String originalEmail = message.getCustomerEmail();
        String normalizedEmail = normalizeEmail(originalEmail);
        if (!originalEmail.equals(normalizedEmail)) {
            changes.put("customerEmail", originalEmail + " → " + normalizedEmail);
        }

        if (!changes.isEmpty()) {
            log.info("OrderRawMessage normalized: orderId={}, changes={}", message.getOrderId(), changes);
        }

        return message.toBuilder()
                .status(normalizedStatus)
                .customerName(normalizedName)
                .customerEmail(normalizedEmail)
                .build();
    }


    public CartRawMessage normalize(CartRawMessage message) {
        if (message == null) return null;
        
        return message;
    }


    public InvoiceRawMessage normalize(InvoiceRawMessage message) {
        if (message == null) return null;

        Map<String, String> changes = new HashMap<>();

        // Normalize status
        String originalStatus = message.getStatus();
        String normalizedStatus = normalizeStatus(originalStatus, INVOICE_STATUS_MAP);
        if (!originalStatus.equals(normalizedStatus)) {
            changes.put("status", originalStatus + " → " + normalizedStatus);
        }

        if (!changes.isEmpty()) {
            log.info("InvoiceRawMessage normalized: invoiceId={}, changes={}", message.getInvoiceId(), changes);
        }

        return message.toBuilder()
                .status(normalizedStatus)
                .build();
    }


    public OrderItemRawMessage normalize(OrderItemRawMessage message) {
        if (message == null) return null;
        
        return message;
    }

    private String normalizePhone(String phone) {
        if (!StringUtils.hasText(phone)) {
            return phone;
        }
        return phone.replaceAll("\\D", "");
    }

    private String normalizeEmail(String email) {
        if (!StringUtils.hasText(email)) {
            return email;
        }
        return email.trim().toLowerCase(Locale.ROOT);
    }


    private String normalizeStatus(String status, Map<String, String> statusMap) {
        if (!StringUtils.hasText(status)) {
            return status;
        }
        
        String uppercased = status.trim().toUpperCase(Locale.ROOT);
        
        if (statusMap.containsValue(uppercased)) {
            return uppercased;
        }
      
        return statusMap.getOrDefault(uppercased, uppercased);
    }

    private String normalizePersonName(String value) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        
        String normalized = Normalizer.normalize(value.trim(), Normalizer.Form.NFC);
        
        String lowercased = normalized.toLowerCase(Locale.forLanguageTag("vi-VN"));
        
        String[] parts = lowercased.split("\\s+");
        
        return String.join(" ", java.util.Arrays.stream(parts)
                .map(part -> {
                    if (part.isEmpty()) {
                        return part;
                    }
                    int firstCodePoint = part.codePointAt(0);
                    String firstChar = new String(Character.toChars(firstCodePoint));
                    String rest = part.substring(Character.charCount(firstCodePoint));
                    return firstChar.toUpperCase(Locale.forLanguageTag("vi-VN")) + rest;
                })
                .collect(Collectors.toList()));
    }
}
