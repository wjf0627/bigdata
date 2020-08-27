package com.jinfeng.clickhouse.utils;

import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @package: com.jinfeng.clickhouse
 * @author: wangjf
 * @date: 2019-07-19
 * @time: 17:24
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class ClickHouseJdbcUtil {

    public static void main(String[] args) throws SQLException {

        ClickHouseDataSource dataSource = new ClickHouseDataSource(
                "jdbc:clickhouse://localhost:8123/wangjf");
        ClickHouseConnectionImpl connection = (ClickHouseConnectionImpl) dataSource.getConnection();

        final String sql = "SELECT * FROM wangjf.device_set LIMIT 10";

        connection.setCatalog("wangjf");
        Statement statement = connection.createStatement();
        //  connection.setCatalog("default");
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getString("device_id"));
        }
        //  resultSet.next();
    }
}
