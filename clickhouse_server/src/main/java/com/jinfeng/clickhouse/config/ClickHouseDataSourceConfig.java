package com.jinfeng.clickhouse.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.jinfeng.clickhouse.utils.PropertyUtil;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;
import java.util.Properties;


/**
 * @package: com.mobvista.server.api.config
 * @author: wangjf
 * @date: 2019-07-22
 * @time: 14:20
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */

@Configuration
@PropertySource(value = {"classpath:jdbc.properties"})
@MapperScan(basePackages = "com.jinfeng.clickhouse.mapper", sqlSessionFactoryRef = "ckSqlSessionFactory")
public class ClickHouseDataSourceConfig {

    private static String driver = PropertyUtil.getProperty("spring.datasource.driverClassName");
    private static String url = PropertyUtil.getProperty("spring.datasource.url");
    private static String username = PropertyUtil.getProperty("spring.datasource.username");
    private static String password = PropertyUtil.getProperty("spring.datasource.password");

    @Bean(name = "ckDataSource")
    public DruidDataSource dataSource() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driver);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaxActive(10);
        dataSource.setInitialSize(1);
        dataSource.setValidationQuery("SELECT 1");
        dataSource.setTestOnBorrow(true);
        Properties properties = new Properties();
        properties.setProperty("socket_timeout", "300000");
        dataSource.setConnectProperties(properties);

        return dataSource;
    }

    @Bean(name = "ckSqlSessionFactory")
    public SqlSessionFactory sqlSessionFactory(@Qualifier("ckDataSource") DataSource dataSource) throws Exception {
        SqlSessionFactoryBean sessionFactoryBean = new SqlSessionFactoryBean();
        sessionFactoryBean.setDataSource(dataSource);
        sessionFactoryBean.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources("classpath*:com/gitee/sunchenbin/mybatis/actable/mapping/*/*.xml"));
        return sessionFactoryBean.getObject();
    }
}