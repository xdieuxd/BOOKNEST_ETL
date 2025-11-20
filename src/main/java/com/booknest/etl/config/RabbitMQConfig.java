package com.booknest.etl.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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

    @Value("${etl.queues.raw}")
    private String rawQueue;

    @Value("${etl.queues.quality}")
    private String qualityQueue;

    @Value("${etl.queues.error}")
    private String errorQueue;

    @Bean
    public DirectExchange etlExchange() {
        return new DirectExchange(etlExchange, true, false);
    }

    @Bean
    public Queue rawQueue() {
        return new Queue(rawQueue, true);
    }

    @Bean
    public Queue qualityQueue() {
        return new Queue(qualityQueue, true);
    }

    @Bean
    public Queue errorQueue() {
        return new Queue(errorQueue, true);
    }

    @Bean
    public Binding rawBinding(DirectExchange etlExchange, Queue rawQueue) {
        return BindingBuilder.bind(rawQueue).to(etlExchange).with("raw");
    }

    @Bean
    public Binding qualityBinding(DirectExchange etlExchange, Queue qualityQueue) {
        return BindingBuilder.bind(qualityQueue).to(etlExchange).with("quality");
    }

    @Bean
    public Binding errorBinding(DirectExchange etlExchange, Queue errorQueue) {
        return BindingBuilder.bind(errorQueue).to(etlExchange).with("error");
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
    public ConnectionFactory connectionFactory(org.springframework.boot.autoconfigure.amqp.RabbitProperties rabbitProperties) {
        CachingConnectionFactory factory = new CachingConnectionFactory(rabbitProperties.getHost(), rabbitProperties.getPort());
        factory.setUsername(rabbitProperties.getUsername());
        factory.setPassword(rabbitProperties.getPassword());
        return factory;
    }
}
