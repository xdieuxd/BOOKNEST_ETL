package com.booknest.etl.service.dq;

import org.springframework.stereotype.Service;
import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;

@Service
public class DataQualityAutoFixService {

    public BookRawMessage fixBook(BookRawMessage book) {
        if (book == null) return null;

        return book.toBuilder()
                .title(trim(book.getTitle()))
                .description(trim(book.getDescription()))
                .status(normalizeStatus(book.getStatus()))
                .build();
    }

    public UserRawMessage fixUser(UserRawMessage user) {
        if (user == null) return null;

        return user.toBuilder()
                .fullName(trim(user.getFullName()))
                .email(trim(user.getEmail()).toLowerCase())
                .phone(normalizePhone(user.getPhone()))
                .status(normalizeStatus(user.getStatus()))
                .build();
    }

    public OrderRawMessage fixOrder(OrderRawMessage order) {
        if (order == null) return null;

        return order.toBuilder()
                .customerName(trim(order.getCustomerName()))
                .customerEmail(trim(order.getCustomerEmail()).toLowerCase())
                .status(normalizeStatus(order.getStatus()))
                .paymentMethod(normalizeStatus(order.getPaymentMethod()))  // Uppercase ONLINE/COD
                .build();
    }

    public CartRawMessage fixCart(CartRawMessage cart) {
        if (cart == null) return null;

        return cart.toBuilder()
                .cartId(trim(cart.getCartId()))
                .customerId(trim(cart.getCustomerId()))
                .build();
    }

    public InvoiceRawMessage fixInvoice(InvoiceRawMessage invoice) {
        if (invoice == null) return null;

        return invoice.toBuilder()
                .invoiceId(trim(invoice.getInvoiceId()))
                .orderId(trim(invoice.getOrderId()))
                .status(normalizeStatus(invoice.getStatus()))
                .build();
    }

    public com.booknest.etl.dto.OrderItemRawMessage fixOrderItem(com.booknest.etl.dto.OrderItemRawMessage item) {
        if (item == null) return null;

        return item.toBuilder()
                .bookId(trim(item.getBookId()))
                .build();
    }

    private String trim(String value) {
        return value != null ? value.strip() : "";
    }

    private String normalizePhone(String phone) {
        if (phone == null) return "";
        return phone.replaceAll("[^0-9]", "");
    }

    private String normalizeStatus(String status) {
        if (status == null) return "";
        return status.strip().toUpperCase();
    }

    public boolean isFixable(String errorRule) {
        if (errorRule == null) return false;
        return errorRule.contains("BLANK")     // Có thể sửa bằng trim
                || errorRule.contains("TRIM")
                || errorRule.contains("LOWERCASE")
                || errorRule.contains("UPPERCASE");
    }
}
