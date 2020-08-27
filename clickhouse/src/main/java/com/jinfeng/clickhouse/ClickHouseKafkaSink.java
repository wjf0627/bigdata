package com.jinfeng.clickhouse;

import com.alibaba.fastjson.JSON;
import com.jinfeng.clickhouse.pojo.CkDemo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * @package: com.jinfeng.kafka
 * @author: wangjf
 * @date: 2019/1/18
 * @time: 下午7:14
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class ClickHouseKafkaSink {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "kafka_topic";

    public static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        //  key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //  value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        CkDemo ckDemo = new CkDemo();
        ckDemo.setTimestamp(System.currentTimeMillis());
        Random random = new Random();
        ckDemo.setLevel(String.valueOf(random.nextInt(5)));
        ckDemo.setMessage(UUID.randomUUID().toString());

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(ckDemo));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(ckDemo));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
