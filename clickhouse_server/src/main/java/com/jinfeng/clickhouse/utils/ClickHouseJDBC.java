package com.jinfeng.clickhouse.utils;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.Random;

/**
 * @package: com.jinfeng.clickhouse.utils
 * @author: wangjf
 * @date: 2020/10/23
 * @time: 10:48 上午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class ClickHouseJDBC {
    public static final String[] SET_VALUES = PropertyUtil.getProperty("datasource.clickhouse.ip").split(",");
    public static final String DRIVER = PropertyUtil.getProperty("datasource.clickhouse.driverClassName");
    public static final String URL = PropertyUtil.getProperty("datasource.clickhouse.url");
    public static final String USERNAME = PropertyUtil.getProperty("datasource.clickhouse.username");
    public static final String PASSWORD = PropertyUtil.getProperty("datasource.clickhouse.password");
    public static final String DATABASE = PropertyUtil.getProperty("datasource.clickhouse.database");
    public static final int TIMEOUT = Integer.parseInt(PropertyUtil.getProperty("datasource.clickhouse.timeout"));

    public static ClickHouseConnection connection() throws InterruptedException, SQLException {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(USERNAME);
        properties.setPassword(PASSWORD);
        properties.setDatabase(DATABASE);
        properties.setSocketTimeout(TIMEOUT);
        properties.setConnectionTimeout(TIMEOUT);

        Random random = new Random();
        int shard = random.nextInt(3);
        String[] ips = SET_VALUES[shard].split(":");
        ClickHouseDataSource dataSource = new ClickHouseDataSource(URL.replace("host", ips[new Random().nextInt(2)]), properties);
        ClickHouseConnection connection;
        try {
            connection = dataSource.getConnection();
        } catch (ClickHouseException e) {
            Thread.sleep(100);
            //  ipId = random.nextInt(3);
            ips = SET_VALUES[shard].split(":");
            dataSource = new ClickHouseDataSource(URL.replace("host", ips[new Random().nextInt(2)]), properties);
            connection = dataSource.getConnection();
        }
        return connection;
    }
}

