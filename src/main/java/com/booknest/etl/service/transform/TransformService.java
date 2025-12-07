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
        // Transform empty title to "Unknown"
        String title = input.getTitle() == null || input.getTitle().trim().isEmpty() 
            ? "Unknown" 
            : capitalizeFirstLetter(input.getTitle());
        
        // Transform empty description to "N/A"
        String description = input.getDescription() == null || input.getDescription().trim().isEmpty()
            ? "N/A"
            : trim(input.getDescription());
        
        // Transform empty authors to ["Unknown"]
        List<String> authors = input.getAuthors() == null || input.getAuthors().isEmpty()
            ? List.of("Unknown")
            : input.getAuthors().stream()
                .map(a -> a == null || a.trim().isEmpty() ? "Unknown" : normalizePersonName(a))
                .collect(Collectors.toList());
        
        // Transform empty categories to ["Uncategorized"]
        List<String> categories = input.getCategories() == null || input.getCategories().isEmpty()
            ? List.of("Uncategorized")
            : input.getCategories().stream()
                .map(c -> c == null || c.trim().isEmpty() ? "Uncategorized" : trim(c))
                .collect(Collectors.toList());
        
        return input.toBuilder()
                .title(title)
                .description(description)
                .authors(authors)
                .categories(categories)
                .status(transformStatus(input.getStatus()))
                .build();
    }

    public UserRawMessage transformUser(UserRawMessage input) {
        // Transform empty fullName to "Unknown"
        String fullName = input.getFullName() == null || input.getFullName().trim().isEmpty()
            ? "Unknown"
            : normalizePersonName(input.getFullName());
        
        // Transform empty email to "unknown@example.com"
        String email = input.getEmail() == null || input.getEmail().trim().isEmpty()
            ? "unknown@example.com"
            : lowerCase(input.getEmail());
        
        // Transform empty phone to "N/A"
        String phone = input.getPhone() == null || input.getPhone().trim().isEmpty()
            ? "N/A"
            : normalizePhone(input.getPhone());
        
        // Transform empty roles to ["guest"]
        List<String> roles = input.getRoles() == null || input.getRoles().isEmpty()
            ? List.of("guest")
            : input.getRoles().stream()
                .map(r -> r == null || r.trim().isEmpty() ? "guest" : r.trim())
                .collect(Collectors.toList());
        
        return input.toBuilder()
                .fullName(fullName)
                .email(email)
                .phone(phone)
                .status(transformStatus(input.getStatus()))
                .roles(roles)
                .build();
    }

    public OrderRawMessage transformOrder(OrderRawMessage input) {
        BigDecimal total = input.getItems() == null ? input.getTotalAmount()
                : input.getItems().stream()
                .map(item -> item.getUnitPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .add(defaultValue(input.getShippingFee()))
                .subtract(defaultValue(input.getDiscount()));

        // Transform empty customerName to "Unknown Customer"
        String customerName = input.getCustomerName() == null || input.getCustomerName().trim().isEmpty()
            ? "Unknown Customer"
            : normalizePersonName(input.getCustomerName());
        
        // Transform empty customerEmail to "unknown@example.com"
        String customerEmail = input.getCustomerEmail() == null || input.getCustomerEmail().trim().isEmpty()
            ? "unknown@example.com"
            : lowerCase(input.getCustomerEmail());

        return input.toBuilder()
                .customerName(customerName)
                .customerEmail(customerEmail)
                .totalAmount(total)
                .items(input.getItems() == null ? input.getItems()
                        : input.getItems().stream().map(this::transformOrderItem).toList())
                .status(transformStatus(input.getStatus()))
                .build();
    }

    private OrderItemRawMessage transformOrderItem(OrderItemRawMessage item) {
        String bookId = item.getBookId() == null || item.getBookId().trim().isEmpty()
            ? "UNKNOWN"
            : trim(item.getBookId());
        
        return item.toBuilder()
                .bookId(bookId)
                .build();
    }

    public OrderItemRawMessage transformOrderItemPublic(OrderItemRawMessage item) {
        return transformOrderItem(item);
    }

    public CartRawMessage transformCart(CartRawMessage input) {
        String customerId = input.getCustomerId() == null || input.getCustomerId().trim().isEmpty()
            ? "UNKNOWN"
            : trim(input.getCustomerId());
        
        return input.toBuilder()
                .customerId(customerId)
                .items(input.getItems() == null ? List.of() :
                        input.getItems().stream().map(this::transformCartItem).toList())
                .build();
    }

    private CartItemRawMessage transformCartItem(CartItemRawMessage item) {
        String bookId = item.getBookId() == null || item.getBookId().trim().isEmpty()
            ? "UNKNOWN"
            : trim(item.getBookId());
        
        return item.toBuilder()
                .bookId(bookId)
                .build();
    }

    public InvoiceRawMessage transformInvoice(InvoiceRawMessage invoice) {
        String orderId = invoice.getOrderId() == null || invoice.getOrderId().trim().isEmpty()
            ? "UNKNOWN"
            : trim(invoice.getOrderId());
        
        return invoice.toBuilder()
                .orderId(orderId)
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

    /**
     * Transform empty/null string fields to "Unknown" for better presentation
     */
    private String toUnknownIfEmpty(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "Unknown";
        }
        return value.trim();
    }

    /**
     * Transform empty/null string fields to "N/A" for optional fields
     */
    private String toNAIfEmpty(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "N/A";
        }
        return value.trim();
    }
}
