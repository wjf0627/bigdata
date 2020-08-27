package com.jinfeng.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.jinfeng.common.entity.Metric;
import com.jinfeng.kafka.utils.pojo.Wikipedia;
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
public class DruidKafkaSink {
    public static final String broker_list = "localhost:9092";
    //  kafka topic，Flink 程序中需要和这个统一
    public static final String topic = "wikipedia";

    public static void writeToKafka() {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("bootstrap.servers", broker_list);
        //  key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //  value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Wikipedia wikipedia = new Wikipedia();

        //  Random random = new Random();

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(wikipedia));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(wikipedia));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
