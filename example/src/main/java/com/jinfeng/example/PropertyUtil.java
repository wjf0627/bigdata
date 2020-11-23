package com.jinfeng.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @package: com.mobvista.dataplatform.utils
 * @author: wangjf
 * @date: 2019-08-26
 * @time: 16:06
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class PropertyUtil {
    private static final Logger logger = LoggerFactory.getLogger(PropertyUtil.class);
    private static Properties props;

    synchronized static private void loadProps(String config) {
        logger.info("start to load properties.......");
        props = new Properties();
        InputStream in = null;
        try {

            in = PropertyUtil.class.getClassLoader().
                    getResourceAsStream(config);
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

    public static String getProperty(String config, String key) {
        if (null == props) {
            loadProps(config);
        }
        return props.getProperty(key);
    }
}