package com.booknest.etl.config;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;

@Configuration
public class DataSourceConfig {

    private static final Logger log = LoggerFactory.getLogger(DataSourceConfig.class);

    @Value("${STAGING_DB_URL:jdbc:mysql://${STAGING_DB_HOST:mysql-source}:3306/staging_db}")
    private String stagingUrl;

    @Value("${STAGING_DB_USERNAME:root}")
    private String stagingUsername;

    @Value("${STAGING_DB_PASSWORD:root}")
    private String stagingPassword;

    @Primary
    @Bean
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties sourceDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Primary
    @Bean
    public DataSource sourceDataSource() {
        DataSource ds = sourceDataSourceProperties().initializeDataSourceBuilder().build();
        try {
            log.info("Source DataSource URL={}", sourceDataSourceProperties().getUrl());
        } catch (Exception e) {
            log.warn("Unable to read source datasource url", e);
        }
        return ds;
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
        try {
            log.info("Creating staging DataSource from STAGING_DB_URL={}", stagingUrl);
            DataSource ds = DataSourceBuilder.create()
                    .driverClassName(stagingDataSourceProperties().getDriverClassName())
                    .url(stagingUrl)
                    .username(stagingUsername)
                    .password(stagingPassword)
                    .build();
            return ds;
        } catch (Exception e) {
            log.warn("Failed to create staging DataSource from env, falling back to properties", e);
            DataSource ds = stagingDataSourceProperties().initializeDataSourceBuilder().build();
            return ds;
        }
    }

    @Bean
    public JdbcTemplate stagingJdbcTemplate(DataSource stagingDataSource) {
        return new JdbcTemplate(stagingDataSource);
    }
}
