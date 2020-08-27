package com.jinfeng.clickhouse.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;

/**
 * @package: com.jinfeng.clickhouse.config
 * @author: wangjf
 * @date: 2019-07-22
 * @time: 11:58
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
@Configuration
@PropertySource(value = {"classpath:jdbc.properties"})
public class DataSourceConfig {

    @Value("ru.yandex.clickhouse.ClickHouseDrive")
    private String driver;

    @Value("jdbc:clickhouse://52.20.184.110:8123/dwh?characterEncoding=utf8")
    private String url;

    @Value("clickhouse")
    private String username;

    @Value("clickhouse")
    private String password;

    @Bean
    @Primary
    public DruidDataSource dataSource() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driver);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaxActive(30);
        dataSource.setInitialSize(10);
        dataSource.setValidationQuery("SELECT 1");
        dataSource.setTestOnBorrow(true);
        return dataSource;
    }

}
