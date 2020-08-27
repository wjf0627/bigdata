package com.jinfeng.clickhouse;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableTransactionManagement
@ServletComponentScan
@SpringBootApplication
@MapperScan("com.jinfeng.clickhouse.dao")
public class ClickhouseServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClickhouseServerApplication.class, args);
    }

}
