package com.jinfeng.clickhouse.utils;

import com.jinfeng.clickhouse.config.ClickHouseDataSourceConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @package: com.jinfeng.clickhouse_server.utils
 * @author: wangjf
 * @date: 2019-07-19
 * @time: 18:10
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class ClickHouseJdbcUtil {
    public static void main(String[] args) throws SQLException {
        ClickHouseDataSourceConfig clickHouseDataSourceConfig = new ClickHouseDataSourceConfig();
        DataSource dataSource = clickHouseDataSourceConfig.dataSource();
        Connection connection = dataSource.getConnection();
        //  DataSourceConfig dataSourceConfig = new DataSourceConfig();
        //  DruidDataSource druidDataSource = dataSourceConfig.dataSource();
        //  Connection connection = druidDataSource.getConnection();
        connection.setCatalog("wangjf");
        Statement statement = connection.createStatement();
        final String sql = "SELECT device_id FROM wangjf.device_primary LIMIT 2000";

        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getString("device_id"));
        }
    }
}
