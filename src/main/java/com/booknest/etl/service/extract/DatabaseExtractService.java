package com.booknest.etl.service.extract;

import java.util.List;

import org.springframework.stereotype.Service;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.repository.book.BookJdbcRepository;
import com.booknest.etl.repository.cart.CartJdbcRepository;
import com.booknest.etl.repository.invoice.InvoiceJdbcRepository;
import com.booknest.etl.repository.user.UserJdbcRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DatabaseExtractService {

    private final BookJdbcRepository bookJdbcRepository;
    private final UserJdbcRepository userJdbcRepository;
    private final CartJdbcRepository cartJdbcRepository;
    private final InvoiceJdbcRepository invoiceJdbcRepository;

    public List<BookRawMessage> fetchBooks() {
        return bookJdbcRepository.findAllBooks();
    }

    public List<UserRawMessage> fetchUsers() {
        return userJdbcRepository.findAllUsers();
    }

    public List<CartRawMessage> fetchCarts() {
        return cartJdbcRepository.findAll();
    }

    public List<InvoiceRawMessage> fetchInvoices() {
        return invoiceJdbcRepository.findAll();
    }
}
