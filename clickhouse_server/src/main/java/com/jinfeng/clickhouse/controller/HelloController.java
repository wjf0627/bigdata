package com.jinfeng.clickhouse.controller;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jinfeng.clickhouse.config.DataSourceConfig;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @package: com.jinfeng.clickhouse.controller
 * @author: wangjf
 * @date: 2019-07-22
 * @time: 11:47
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
@Controller
@RequestMapping("")
public class HelloController {

    @GetMapping("/hello")
    @ResponseBody
    public JSONArray hello() throws SQLException {
        JSONArray jsonArray = new JSONArray();

        //  DataSourceConfig clickHouseDataSourceConfig = new ClickHouseDataSourceConfig();

        DataSourceConfig dataSourceConfig = new DataSourceConfig();
        DruidDataSource druidDataSource = dataSourceConfig.dataSource();
        Connection connection = druidDataSource.getConnection();
        connection.setCatalog("dwh");
        Statement statement = connection.createStatement();
        final String sql = "SELECT * FROM dwh.ods_user_info_all LIMIT 10";

        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("device_id", resultSet.getString("device_id"));
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }
}
