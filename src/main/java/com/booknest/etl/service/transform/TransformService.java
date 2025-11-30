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
                .title(trim(input.getTitle()))  // Giữ nguyên tên sách, chỉ trim
                .description(trim(input.getDescription()))
                .authors(input.getAuthors().stream().map(this::trim).collect(Collectors.toList()))  // Giữ nguyên tên tác giả
                .categories(input.getCategories().stream().map(this::trim).collect(Collectors.toList()))
                .status(transformStatus(input.getStatus()))
                .build();
    }

    public UserRawMessage transformUser(UserRawMessage input) {
        return input.toBuilder()
                .fullName(trim(input.getFullName()))  // Chỉ trim, giữ nguyên case gốc
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
                .customerName(trim(input.getCustomerName()))  // Chỉ trim, giữ nguyên case gốc
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
     * Chuẩn hóa tên người: trim, lowercase, sau đó viết hoa chữ cái đầu mỗi từ
     * VD: "nguyễn   văn   A" → "Nguyễn Văn A"
     * CHỈ dùng cho tên người (customer, user), KHÔNG dùng cho title sách!
     */
    private String normalizePersonName(String value) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        String trimmed = value.trim().toLowerCase(Locale.getDefault());
        String[] parts = trimmed.split("\\s+");
        return String.join(" ", java.util.Arrays.stream(parts)
                .map(part -> part.isEmpty() ? part
                        : part.substring(0, 1).toUpperCase(Locale.getDefault()) + part.substring(1))
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
