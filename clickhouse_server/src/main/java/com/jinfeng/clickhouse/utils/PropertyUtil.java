package com.jinfeng.clickhouse.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @package: com.jinfeng.clickhouse.utils
 * @author: wangjf
 * @date: 2019-08-30
 * @time: 22:05
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class PropertyUtil {
    private static final Logger logger = LoggerFactory.getLogger(PropertyUtil.class);
    private static Properties props;

    static {
        loadProps();
    }

    synchronized static private void loadProps() {
        logger.info("start to load properties.......");
        props = new Properties();
        InputStream in = null;
        try {

            in = PropertyUtil.class.getClassLoader().
                    getResourceAsStream("jdbc.properties");
            props.load(in);
        } catch (IOException e) {
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
            } catch (IOException e) {
            }
        }
    }

    public static String getProperty(String key) {
        if (null == props) {
            loadProps();
        }
        return props.getProperty(key);
    }
}
