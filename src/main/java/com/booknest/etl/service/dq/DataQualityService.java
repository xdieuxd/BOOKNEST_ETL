package com.booknest.etl.service.dq;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.stereotype.Service;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartItemRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.DqErrorDto;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderItemRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.dq.rule.DataQualityRuleChain;
import com.booknest.etl.dq.rule.book.FreePriceConsistencyRule;
import com.booknest.etl.dq.rule.common.CollectionMinSizeRule;
import com.booknest.etl.dq.rule.common.DateNotInFutureRule;
import com.booknest.etl.dq.rule.common.DateTimeNotInFutureRule;
import com.booknest.etl.dq.rule.common.MaxLengthRule;
import com.booknest.etl.dq.rule.common.NotBlankRule;
import com.booknest.etl.dq.rule.common.NotNullRule;
import com.booknest.etl.dq.rule.common.PositiveDecimalRule;
import com.booknest.etl.dq.rule.common.RegexRule;
import com.booknest.etl.dq.rule.common.StringSetRule;

@Service
public class DataQualityService {

    private static final Pattern NAME_PATTERN = Pattern.compile("^[\\p{L}][\\p{L} .'-]{1,99}$");
    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern PHONE_PATTERN = Pattern.compile("^[0-9]{9,15}$");

    private final DataQualityRuleChain<BookRawMessage> bookRuleChain;
    private final DataQualityRuleChain<UserRawMessage> userRuleChain;
    private final DataQualityRuleChain<OrderRawMessage> orderRuleChain;
    private final DataQualityRuleChain<CartRawMessage> cartRuleChain;
    private final DataQualityRuleChain<InvoiceRawMessage> invoiceRuleChain;

    public DataQualityService() {
        this.bookRuleChain = buildBookRules();
        this.userRuleChain = buildUserRules();
        this.orderRuleChain = buildOrderRules();
        this.cartRuleChain = buildCartRules();
        this.invoiceRuleChain = buildInvoiceRules();
    }

    public List<DqErrorDto> validateBook(BookRawMessage book) {
        return bookRuleChain.validate(book);
    }

    public List<DqErrorDto> validateUser(UserRawMessage user) {
        return userRuleChain.validate(user);
    }

    public List<DqErrorDto> validateOrder(OrderRawMessage order) {
        List<DqErrorDto> errors = new ArrayList<>(orderRuleChain.validate(order));
        validateOrderItems(order, errors);
        validateTotals(order, errors);
        return errors;
    }

    public List<DqErrorDto> validateCart(CartRawMessage cart) {
        List<DqErrorDto> errors = new ArrayList<>(cartRuleChain.validate(cart));
        if (cart.getItems() == null || cart.getItems().isEmpty()) {
            errors.add(DqErrorDto.builder()
                    .field("items")
                    .rule("MIN_SIZE")
                    .message("Giỏ hàng phải có ít nhất một sản phẩm")
                    .build());
        } else {
            int index = 0;
            for (CartItemRawMessage item : cart.getItems()) {
                if (item.getBookId() == null || item.getBookId().isBlank()) {
                    errors.add(DqErrorDto.builder()
                            .field("items[" + index + "].bookId")
                            .rule("NOT_BLANK")
                            .message("Mã sách không được trống")
                            .build());
                }
                if (item.getQuantity() == null || item.getQuantity() <= 0) {
                    errors.add(DqErrorDto.builder()
                            .field("items[" + index + "].quantity")
                            .rule("POSITIVE_INT")
                            .message("Số lượng phải > 0")
                            .build());
                }
                index++;
            }
        }
        return errors;
    }

    public List<DqErrorDto> validateInvoice(InvoiceRawMessage invoice) {
        return invoiceRuleChain.validate(invoice);
    }

    private void validateOrderItems(OrderRawMessage order, List<DqErrorDto> errors) {
        if (order.getItems() == null || order.getItems().isEmpty()) {
            errors.add(DqErrorDto.builder()
                    .field("items")
                    .rule("MIN_SIZE")
                    .message("Đơn hàng phải có ít nhất một sản phẩm")
                    .build());
            return;
        }
        int index = 0;
        for (OrderItemRawMessage item : order.getItems()) {
            String baseField = "items[" + index + "]";
            if (item.getBookId() == null || item.getBookId().isBlank()) {
                errors.add(DqErrorDto.builder()
                        .field(baseField + ".bookId")
                        .rule("NOT_BLANK")
                        .message("Mã sách không được trống")
                        .build());
            }
            if (item.getQuantity() == null || item.getQuantity() <= 0) {
                errors.add(DqErrorDto.builder()
                        .field(baseField + ".quantity")
                        .rule("POSITIVE_INT")
                        .message("Số lượng phải > 0")
                        .build());
            }
            if (item.getUnitPrice() == null || item.getUnitPrice().compareTo(BigDecimal.ZERO) <= 0) {
                errors.add(DqErrorDto.builder()
                        .field(baseField + ".unitPrice")
                        .rule("POSITIVE")
                        .message("Đơn giá phải > 0")
                        .build());
            }
            index++;
        }
    }

    private void validateTotals(OrderRawMessage order, List<DqErrorDto> errors) {
        if (order.getItems() == null || order.getItems().isEmpty() || order.getTotalAmount() == null) {
            return;
        }
        BigDecimal sumLines = order.getItems().stream()
                .filter(i -> i.getUnitPrice() != null && i.getQuantity() != null)
                .map(i -> i.getUnitPrice().multiply(BigDecimal.valueOf(i.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal expectedTotal = sumLines
                .add(defaultValue(order.getShippingFee()))
                .subtract(defaultValue(order.getDiscount()));
        if (expectedTotal.compareTo(order.getTotalAmount()) != 0) {
            errors.add(DqErrorDto.builder()
                    .field("totalAmount")
                    .rule("TOTAL_MISMATCH")
                    .message("Tổng tiền không khớp tổng dòng + phí vận chuyển - chiết khấu")
                    .build());
        }
    }

    private BigDecimal defaultValue(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    private DataQualityRuleChain<BookRawMessage> buildBookRules() {
        return new DataQualityRuleChain<BookRawMessage>()
                .addRule(new NotBlankRule<>("bookId", "Mã sách không được trống", BookRawMessage::getBookId))
                .addRule(new NotBlankRule<>("title", "Tên sách không được trống", BookRawMessage::getTitle))
                .addRule(new MaxLengthRule<>("title", 300, "Tên sách tối đa 300 ký tự", BookRawMessage::getTitle))
                .addRule(new MaxLengthRule<>("description", 2000, "Mô tả tối đa 2000 ký tự", BookRawMessage::getDescription))
                .addRule(new CollectionMinSizeRule<>("authors", 1, "Phải có ít nhất 1 tác giả", BookRawMessage::getAuthors))
                .addRule(new CollectionMinSizeRule<>("categories", 1, "Phải có ít nhất 1 thể loại", BookRawMessage::getCategories))
                .addRule(new NotNullRule<>("price", "Giá bán không được trống", BookRawMessage::getPrice))
                .addRule(new PositiveDecimalRule<>("price", true, "Giá bán phải >= 0", BookRawMessage::getPrice))
                .addRule(new NotNullRule<>("status", "Trạng thái không được trống", BookRawMessage::getStatus))
                .addRule(new MaxLengthRule<>("status", 20, "Trạng thái quá dài", BookRawMessage::getStatus))
                .addRule(new StringSetRule<>("status", Set.of("AN", "HIEU_LUC"), "Trạng thái sách không hợp lệ", BookRawMessage::getStatus))
                .addRule(new DateNotInFutureRule<>("releasedAt", "Ngày phát hành không được vượt hiện tại", BookRawMessage::getReleasedAt))
                .addRule(new FreePriceConsistencyRule());
    }

    private DataQualityRuleChain<UserRawMessage> buildUserRules() {
        return new DataQualityRuleChain<UserRawMessage>()
                .addRule(new NotBlankRule<>("userId", "Mã khách hàng không được trống", UserRawMessage::getUserId))
                .addRule(new MaxLengthRule<>("userId", 50, "Mã khách hàng tối đa 50 ký tự", UserRawMessage::getUserId))
                .addRule(new NotBlankRule<>("fullName", "Họ tên không được trống", UserRawMessage::getFullName))
                .addRule(new RegexRule<>("fullName", "Họ tên chứa ký tự không hợp lệ", NAME_PATTERN, UserRawMessage::getFullName))
                .addRule(new MaxLengthRule<>("fullName", 150, "Họ tên tối đa 150 ký tự", UserRawMessage::getFullName))
                .addRule(new NotBlankRule<>("email", "Email không được trống", UserRawMessage::getEmail))
                .addRule(new RegexRule<>("email", "Email không hợp lệ", EMAIL_PATTERN, UserRawMessage::getEmail))
                .addRule(new MaxLengthRule<>("email", 150, "Email tối đa 150 ký tự", UserRawMessage::getEmail))
                .addRule(new NotBlankRule<>("phone", "Số điện thoại không được trống", UserRawMessage::getPhone))
                .addRule(new RegexRule<>("phone", "Số điện thoại phải gồm 9-15 chữ số", PHONE_PATTERN, UserRawMessage::getPhone))
                .addRule(new MaxLengthRule<>("phone", 20, "Số điện thoại tối đa 20 ký tự", UserRawMessage::getPhone))
                .addRule(new CollectionMinSizeRule<>("roles", 1, "Phải có ít nhất 1 vai trò", UserRawMessage::getRoles))
                .addRule(new StringSetRule<>("status", Set.of("HOAT_DONG", "KHOA"), "Trạng thái không hợp lệ", UserRawMessage::getStatus));
    }

    private DataQualityRuleChain<OrderRawMessage> buildOrderRules() {
        return new DataQualityRuleChain<OrderRawMessage>()
                .addRule(new NotBlankRule<>("orderId", "Mã đơn hàng không được trống", OrderRawMessage::getOrderId))
                .addRule(new MaxLengthRule<>("orderId", 50, "Mã đơn hàng tối đa 50 ký tự", OrderRawMessage::getOrderId))
                .addRule(new NotBlankRule<>("customerEmail", "Đơn hàng phải có email khách", OrderRawMessage::getCustomerEmail))
                .addRule(new RegexRule<>("customerEmail", "Email khách không hợp lệ", EMAIL_PATTERN, OrderRawMessage::getCustomerEmail))
                .addRule(new NotBlankRule<>("customerName", "Tên khách không được trống", OrderRawMessage::getCustomerName))
                .addRule(new MaxLengthRule<>("customerName", 150, "Tên khách tối đa 150 ký tự", OrderRawMessage::getCustomerName))
                .addRule(new CollectionMinSizeRule<>("items", 1, "Đơn hàng phải có ít nhất một sản phẩm", OrderRawMessage::getItems))
                .addRule(new PositiveDecimalRule<>("totalAmount", false, "Tổng tiền phải > 0", OrderRawMessage::getTotalAmount))
                .addRule(new PositiveDecimalRule<>("discount", true, "Chiết khấu không được âm", OrderRawMessage::getDiscount))
                .addRule(new PositiveDecimalRule<>("shippingFee", true, "Phí vận chuyển không được âm", OrderRawMessage::getShippingFee))
                .addRule(new StringSetRule<>("status", Set.of("TAO_MOI","CHO_THANH_TOAN","DA_THANH_TOAN","DANG_GIAO","DA_NHAN","DA_HUY","HOAN_TIEN"),
                        "Trạng thái đơn hàng không hợp lệ", OrderRawMessage::getStatus))
                .addRule(new StringSetRule<>("paymentMethod", Set.of("ONLINE","COD"), "Phương thức thanh toán không hợp lệ", OrderRawMessage::getPaymentMethod))
                .addRule(new DateTimeNotInFutureRule<>("createdAt", "Thời gian tạo đơn không được vượt hiện tại", OrderRawMessage::getCreatedAt))
                .addRule(new DateTimeNotInFutureRule<>("extractedAt", "Thời gian extract không được vượt hiện tại", OrderRawMessage::getExtractedAt));
    }

    private DataQualityRuleChain<CartRawMessage> buildCartRules() {
        return new DataQualityRuleChain<CartRawMessage>()
                .addRule(new NotBlankRule<>("cartId", "Mã giỏ hàng không được trống", CartRawMessage::getCartId))
                .addRule(new NotBlankRule<>("customerId", "Mã khách không được trống", CartRawMessage::getCustomerId));
    }

    private DataQualityRuleChain<InvoiceRawMessage> buildInvoiceRules() {
        return new DataQualityRuleChain<InvoiceRawMessage>()
                .addRule(new NotBlankRule<>("invoiceId", "Mã hóa đơn không được trống", InvoiceRawMessage::getInvoiceId))
                .addRule(new NotBlankRule<>("orderId", "Hóa đơn phải gắn với đơn hàng", InvoiceRawMessage::getOrderId))
                .addRule(new PositiveDecimalRule<>("amount", false, "Số tiền hóa đơn phải > 0", InvoiceRawMessage::getAmount))
                .addRule(new StringSetRule<>("status", Set.of("CHUA_TT", "DA_TT"), "Trạng thái hóa đơn không hợp lệ", InvoiceRawMessage::getStatus));
    }
}
