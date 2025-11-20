package com.booknest.etl.dq.rule.book;

import java.math.BigDecimal;
import java.util.List;

import com.booknest.etl.dq.rule.DataQualityRule;
import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.DqErrorDto;

public class FreePriceConsistencyRule implements DataQualityRule<BookRawMessage> {

    @Override
    public void validate(BookRawMessage payload, List<DqErrorDto> errors) {
        BigDecimal price = payload.getPrice();
        boolean free = payload.isFree();
        if (price == null) {
            return;
        }
        if (free && price.compareTo(BigDecimal.ZERO) != 0) {
            errors.add(DqErrorDto.builder()
                    .field("price")
                    .rule("FREE_PRICE")
                    .message("Sách miễn phí phải có giá = 0")
                    .build());
        }
        if (!free && price.compareTo(BigDecimal.ZERO) <= 0) {
            errors.add(DqErrorDto.builder()
                    .field("price")
                    .rule("FREE_PRICE")
                    .message("Sách có phí phải có giá > 0")
                    .build());
        }
    }
}
