package com.jinfeng.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.jinfeng.common.entity.Metric;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * @package: com.jinfeng.kafka
 * @author: wangjf
 * @date: 2019/1/18
 * @time: 下午7:14
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class KafkaUtils {
    public static final String broker_list = "localhost:9092";
    //  kafka topic，Flink 程序中需要和这个统一
    public static final String topic = "metric";

    public static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        //  key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //  value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();

        tags.put("cluster", "wangjf");
        tags.put("host_ip", "localhost");

        Random random = new Random();
        fields.put("used_percent", random.nextInt(10000));
        fields.put("max", random.nextInt(10000));
        fields.put("used", random.nextInt(100));
        fields.put("init", random.nextInt(5));

        //  metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
