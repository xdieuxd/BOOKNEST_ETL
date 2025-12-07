package com.booknest.etl.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;

@Configuration
@EnableRabbit
public class RabbitMQConfig {

    @Value("${etl.exchange}")
    private String etlExchange;

    // Book queues
    @Value("${etl.queues.book-raw}")
    private String bookRawQueue;
    @Value("${etl.queues.book-quality}")
    private String bookQualityQueue;
    @Value("${etl.queues.book-error}")
    private String bookErrorQueue;

    // Customer queues
    @Value("${etl.queues.customer-raw}")
    private String customerRawQueue;
    @Value("${etl.queues.customer-quality}")
    private String customerQualityQueue;
    @Value("${etl.queues.customer-error}")
    private String customerErrorQueue;

    // Order queues
    @Value("${etl.queues.order-raw}")
    private String orderRawQueue;
    @Value("${etl.queues.order-quality}")
    private String orderQualityQueue;
    @Value("${etl.queues.order-error}")
    private String orderErrorQueue;

    // Order Item queues
    @Value("${etl.queues.orderitem-raw}")
    private String orderItemRawQueue;
    @Value("${etl.queues.orderitem-quality}")
    private String orderItemQualityQueue;
    @Value("${etl.queues.orderitem-error}")
    private String orderItemErrorQueue;

    // Cart queues
    @Value("${etl.queues.cart-raw}")
    private String cartRawQueue;
    @Value("${etl.queues.cart-quality}")
    private String cartQualityQueue;
    @Value("${etl.queues.cart-error}")
    private String cartErrorQueue;

    // Invoice queues
    @Value("${etl.queues.invoice-raw}")
    private String invoiceRawQueue;
    @Value("${etl.queues.invoice-quality}")
    private String invoiceQualityQueue;
    @Value("${etl.queues.invoice-error}")
    private String invoiceErrorQueue;

    @Bean
    public DirectExchange etlExchange() {
        return new DirectExchange(etlExchange, true, false);
    }

    // ========== Book Queues ==========
    @Bean
    public Queue bookRawQueue() {
        return new Queue(bookRawQueue, true);
    }

    @Bean
    public Queue bookQualityQueue() {
        return new Queue(bookQualityQueue, true);
    }

    @Bean
    public Queue bookErrorQueue() {
        return new Queue(bookErrorQueue, true);
    }

    @Bean
    public Binding bookRawBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(bookRawQueue()).to(etlExchange).with("book.raw");
    }

    @Bean
    public Binding bookQualityBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(bookQualityQueue()).to(etlExchange).with("book.quality");
    }

    @Bean
    public Binding bookErrorBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(bookErrorQueue()).to(etlExchange).with("book.error");
    }

    // ========== Customer Queues ==========
    @Bean
    public Queue customerRawQueue() {
        return new Queue(customerRawQueue, true);
    }

    @Bean
    public Queue customerQualityQueue() {
        return new Queue(customerQualityQueue, true);
    }

    @Bean
    public Queue customerErrorQueue() {
        return new Queue(customerErrorQueue, true);
    }

    @Bean
    public Binding customerRawBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(customerRawQueue()).to(etlExchange).with("customer.raw");
    }

    @Bean
    public Binding customerQualityBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(customerQualityQueue()).to(etlExchange).with("customer.quality");
    }

    @Bean
    public Binding customerErrorBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(customerErrorQueue()).to(etlExchange).with("customer.error");
    }

    // ========== Order Queues ==========
    @Bean
    public Queue orderRawQueue() {
        return new Queue(orderRawQueue, true);
    }

    @Bean
    public Queue orderQualityQueue() {
        return new Queue(orderQualityQueue, true);
    }

    @Bean
    public Queue orderErrorQueue() {
        return new Queue(orderErrorQueue, true);
    }

    @Bean
    public Binding orderRawBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(orderRawQueue()).to(etlExchange).with("order.raw");
    }

    @Bean
    public Binding orderQualityBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(orderQualityQueue()).to(etlExchange).with("order.quality");
    }

    @Bean
    public Binding orderErrorBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(orderErrorQueue()).to(etlExchange).with("order.error");
    }

    // ========== Order Item Queues ==========
    @Bean
    public Queue orderItemRawQueue() {
        return new Queue(orderItemRawQueue, true);
    }

    @Bean
    public Queue orderItemQualityQueue() {
        return new Queue(orderItemQualityQueue, true);
    }

    @Bean
    public Queue orderItemErrorQueue() {
        return new Queue(orderItemErrorQueue, true);
    }

    @Bean
    public Binding orderItemRawBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(orderItemRawQueue()).to(etlExchange).with("orderitem.raw");
    }

    @Bean
    public Binding orderItemQualityBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(orderItemQualityQueue()).to(etlExchange).with("orderitem.quality");
    }

    @Bean
    public Binding orderItemErrorBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(orderItemErrorQueue()).to(etlExchange).with("orderitem.error");
    }

    // ========== Cart Queues ==========
    @Bean
    public Queue cartRawQueue() {
        return new Queue(cartRawQueue, true);
    }

    @Bean
    public Queue cartQualityQueue() {
        return new Queue(cartQualityQueue, true);
    }

    @Bean
    public Queue cartErrorQueue() {
        return new Queue(cartErrorQueue, true);
    }

    @Bean
    public Binding cartRawBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(cartRawQueue()).to(etlExchange).with("cart.raw");
    }

    @Bean
    public Binding cartQualityBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(cartQualityQueue()).to(etlExchange).with("cart.quality");
    }

    @Bean
    public Binding cartErrorBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(cartErrorQueue()).to(etlExchange).with("cart.error");
    }

    // ========== Invoice Queues ==========
    @Bean
    public Queue invoiceRawQueue() {
        return new Queue(invoiceRawQueue, true);
    }

    @Bean
    public Queue invoiceQualityQueue() {
        return new Queue(invoiceQualityQueue, true);
    }

    @Bean
    public Queue invoiceErrorQueue() {
        return new Queue(invoiceErrorQueue, true);
    }

    @Bean
    public Binding invoiceRawBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(invoiceRawQueue()).to(etlExchange).with("invoice.raw");
    }

    @Bean
    public Binding invoiceQualityBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(invoiceQualityQueue()).to(etlExchange).with("invoice.quality");
    }

    @Bean
    public Binding invoiceErrorBinding(DirectExchange etlExchange) {
        return BindingBuilder.bind(invoiceErrorQueue()).to(etlExchange).with("invoice.error");
    }

    @Bean
    public Jackson2JsonMessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jackson2JsonMessageConverter());
        template.setExchange(etlExchange);
        return template;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory,
                                                                               Jackson2JsonMessageConverter converter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(converter);
        factory.setAutoStartup(true); 
        factory.setConcurrentConsumers(3); 
        factory.setMaxConcurrentConsumers(10);
        
        factory.setErrorHandler(t -> {
            System.err.println("====== RABBITMQ LISTENER ERROR ======");
            System.err.println("Error: " + t.getMessage());
            t.printStackTrace();
            System.err.println("=====================================");
        });
        
        return factory;
    }

    @Bean
    public ConnectionFactory connectionFactory(org.springframework.boot.autoconfigure.amqp.RabbitProperties rabbitProperties) {
        CachingConnectionFactory factory = new CachingConnectionFactory(rabbitProperties.getHost(), rabbitProperties.getPort());
        factory.setUsername(rabbitProperties.getUsername());
        factory.setPassword(rabbitProperties.getPassword());
        return factory;
    }
}
