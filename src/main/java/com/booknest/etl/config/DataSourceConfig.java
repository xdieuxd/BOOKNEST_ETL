package com.booknest.etl.config;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class DataSourceConfig {

    @Primary
    @Bean
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties sourceDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Primary
    @Bean
    public DataSource sourceDataSource() {
        return sourceDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean
    public JdbcTemplate sourceJdbcTemplate(DataSource sourceDataSource) {
        return new JdbcTemplate(sourceDataSource);
    }

    @Bean
    @ConfigurationProperties("staging.datasource")
    public DataSourceProperties stagingDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    public DataSource stagingDataSource() {
        return stagingDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean
    public JdbcTemplate stagingJdbcTemplate(DataSource stagingDataSource) {
        return new JdbcTemplate(stagingDataSource);
    }
}
