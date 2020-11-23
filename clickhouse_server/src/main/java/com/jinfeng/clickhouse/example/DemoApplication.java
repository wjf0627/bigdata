package com.jinfeng.clickhouse.example;

import com.jinfeng.clickhouse.utils.ClickHouseJDBC;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.response.ClickHouseResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @package: com.jinfeng.clickhouse.example
 * @author: wangjf
 * @date: 2020/10/23
 * @time: 10:53 上午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class DemoApplication {
    public static void main(String[] args) throws SQLException, InterruptedException {
        ClickHouseConnection connection = ClickHouseJDBC.connection();

        String sql = "SELECT * FROM wangjf.flink_test WHERE dt = '2020-10-19' LIMIT 10";
        ClickHouseResultSet resultSet = null;
        try {
            resultSet = (ClickHouseResultSet) connection.prepareStatement(sql).executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (resultSet != null && resultSet.hasNext()) {
            while (resultSet.next()) {
                System.out.println(resultSet.getString("device_id"));
            }
        } else {
            resultSet = (ClickHouseResultSet) connection.prepareStatement("SELECT '1' device_id").executeQuery();
            while (resultSet.next()) {
                System.out.println(resultSet.getString("device_id"));
            }
        }
    }
}
