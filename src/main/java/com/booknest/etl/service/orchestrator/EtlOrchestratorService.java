package com.booknest.etl.service.orchestrator;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.booknest.etl.dto.BookRawMessage;
import com.booknest.etl.dto.CartRawMessage;
import com.booknest.etl.dto.InvoiceRawMessage;
import com.booknest.etl.dto.OrderRawMessage;
import com.booknest.etl.dto.UserRawMessage;
import com.booknest.etl.messaging.producer.EtlMessagePublisher;
import com.booknest.etl.service.extract.CsvExtractService;
import com.booknest.etl.service.extract.DatabaseExtractService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class EtlOrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(EtlOrchestratorService.class);

    private final CsvExtractService csvExtractService;
    private final DatabaseExtractService databaseExtractService;
    private final EtlMessagePublisher messagePublisher;

    // TẠẠM TẮT scheduled job để data trong staging không bị ghi đè khi demo
    // @Scheduled(fixedDelayString = "${etl.extract.schedule-fixed-delay:300000}")
    public void scheduleExtractJob() {
        log.info("Starting scheduled extract job");
        runDatabaseExtract();
        runCsvExtract();
        log.info("Completed scheduled extract job");
    }

    public void runDatabaseExtract() {
        List<BookRawMessage> books = databaseExtractService.fetchBooks();
        log.info("Publishing {} book records from database", books.size());
        books.forEach(messagePublisher::sendRaw);

        List<UserRawMessage> users = databaseExtractService.fetchUsers();
        log.info("Publishing {} user records from database", users.size());
        users.forEach(messagePublisher::sendRaw);

        List<CartRawMessage> carts = databaseExtractService.fetchCarts();
        log.info("Publishing {} cart records from database", carts.size());
        carts.forEach(messagePublisher::sendRaw);

        List<InvoiceRawMessage> invoices = databaseExtractService.fetchInvoices();
        log.info("Publishing {} invoice records from database", invoices.size());
        invoices.forEach(messagePublisher::sendRaw);
    }

    public void runCsvExtract() {
        List<BookRawMessage> books = csvExtractService.readBooks();
        log.info("Publishing {} book records from CSV", books.size());
        books.forEach(messagePublisher::sendRaw);

        List<UserRawMessage> customers = csvExtractService.readCustomers();
        log.info("Publishing {} customer records from CSV", customers.size());
        customers.forEach(messagePublisher::sendRaw);

        List<OrderRawMessage> orders = csvExtractService.readOrders();
        log.info("Publishing {} order records from CSV", orders.size());
        orders.forEach(messagePublisher::sendRaw);
    }
}
