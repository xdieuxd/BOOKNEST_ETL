package com.booknest.etl.service.transform;

import java.math.BigDecimal;
import java.text.Normalizer;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartItemRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;

@Service
public class TransformService {

    public BookRawMessage transformBook(BookRawMessage input) {
        return input.toBuilder()
                .title(capitalizeFirstLetter(input.getTitle()))  // Viết hoa chữ cái đầu tiên
                .description(trim(input.getDescription()))
                .authors(input.getAuthors().stream().map(this::normalizePersonName).collect(Collectors.toList()))  // Capitalize từng từ
                .categories(input.getCategories().stream().map(this::trim).collect(Collectors.toList()))
                .status(transformStatus(input.getStatus()))
                .build();
    }

    public UserRawMessage transformUser(UserRawMessage input) {
        return input.toBuilder()
                .fullName(normalizePersonName(input.getFullName()))  // Chuẩn hóa tên người: viết hoa chữ cái đầu
                .email(lowerCase(input.getEmail()))
                .phone(normalizePhone(input.getPhone()))
                .status(transformStatus(input.getStatus()))
                .roles(input.getRoles().stream().map(String::trim).collect(Collectors.toList()))
                .build();
    }

    public OrderRawMessage transformOrder(OrderRawMessage input) {
        BigDecimal total = input.getItems() == null ? input.getTotalAmount()
                : input.getItems().stream()
                .map(item -> item.getUnitPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .add(defaultValue(input.getShippingFee()))
                .subtract(defaultValue(input.getDiscount()));

        return input.toBuilder()
                .customerName(normalizePersonName(input.getCustomerName()))  // Chuẩn hóa tên người: viết hoa chữ cái đầu
                .customerEmail(lowerCase(input.getCustomerEmail()))
                .totalAmount(total)
                .items(input.getItems() == null ? input.getItems()
                        : input.getItems().stream().map(this::transformOrderItem).toList())
                .status(transformStatus(input.getStatus()))
                .build();
    }

    private OrderItemRawMessage transformOrderItem(OrderItemRawMessage item) {
        return item.toBuilder()
                .bookId(trim(item.getBookId()))
                .build();
    }

    public OrderItemRawMessage transformOrderItemPublic(OrderItemRawMessage item) {
        return transformOrderItem(item);
    }

    public CartRawMessage transformCart(CartRawMessage input) {
        return input.toBuilder()
                .customerId(trim(input.getCustomerId()))
                .items(input.getItems() == null ? List.of() :
                        input.getItems().stream().map(this::transformCartItem).toList())
                .build();
    }

    private CartItemRawMessage transformCartItem(CartItemRawMessage item) {
        return item.toBuilder()
                .bookId(trim(item.getBookId()))
                .build();
    }

    public InvoiceRawMessage transformInvoice(InvoiceRawMessage invoice) {
        return invoice.toBuilder()
                .orderId(trim(invoice.getOrderId()))
                .status(transformStatus(invoice.getStatus()))
                .build();
    }

    /**
     * Viết hoa chữ cái đầu tiên của câu, giữ nguyên phần còn lại
     */
    private String capitalizeFirstLetter(String value) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return trimmed;
        }
        
        // Normalize Unicode (NFC form)
        String normalized = Normalizer.normalize(trimmed, Normalizer.Form.NFC);
        
        // Viết hoa ký tự đầu tiên
        int firstCodePoint = normalized.codePointAt(0);
        String firstChar = new String(Character.toChars(firstCodePoint));
        String rest = normalized.substring(Character.charCount(firstCodePoint));
        
        return firstChar.toUpperCase(Locale.forLanguageTag("vi-VN")) + rest.toLowerCase(Locale.forLanguageTag("vi-VN"));
    }

    /**
     * Chuẩn hóa tên người: trim, lowercase, sau đó viết hoa chữ cái đầu mỗi từ
     */
    private String normalizePersonName(String value) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        
        // Bước 1: Normalize Unicode (NFC form) để xử lý đúng dấu tiếng Việt
        String normalized = Normalizer.normalize(value.trim(), Normalizer.Form.NFC);
        
        // Bước 2: Lowercase toàn bộ
        String lowercased = normalized.toLowerCase(Locale.forLanguageTag("vi-VN"));
        
        // Bước 3: Split theo khoảng trắng
        String[] parts = lowercased.split("\\s+");
        
        // Bước 4: Viết hoa chữ cái đầu mỗi từ
        return String.join(" ", java.util.Arrays.stream(parts)
                .map(part -> {
                    if (part.isEmpty()) {
                        return part;
                    }
                    // Xử lý đúng với Unicode tiếng Việt: lấy ký tự đầu tiên (có thể nhiều code unit)
                    int firstCodePoint = part.codePointAt(0);
                    String firstChar = new String(Character.toChars(firstCodePoint));
                    String rest = part.substring(Character.charCount(firstCodePoint));
                    return firstChar.toUpperCase(Locale.forLanguageTag("vi-VN")) + rest;
                })
                .toList());
    }

    private String transformStatus(String status) {
        return status == null ? null : status.trim().toUpperCase(Locale.getDefault());
    }

    private String lowerCase(String value) {
        return value == null ? null : value.trim().toLowerCase(Locale.getDefault());
    }

    private String trim(String value) {
        return value == null ? null : value.trim();
    }

    private String normalizePhone(String phone) {
        if (phone == null) {
            return null;
        }
        String digits = phone.replaceAll("\\D", "");
        return digits;
    }

    private BigDecimal defaultValue(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }
}
