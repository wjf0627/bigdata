package com.jinfeng.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.jinfeng.kafka.utils.pojo.CkDemo;
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
    //  kafka topic，Flink 程序中需要和这个统一
    public static final String topic = "kafka_topic";

    public static final String[] devIds = new String[]{"0000073a-73da-4c31-a7d7-bcc33b4263fd", "00004515-52b1-4f45-85f5-438b238679e7", "0000b0fe-aa44-4d9e-9421-4c52a609b113",
            "00010b77-a388-4c99-ba9e-4773016e33e0", "000124a1-db9b-49a7-adeb-604b50875d31", "00015ca0-300d-4d2d-8744-c471988a91e7", "00017dc4-2122-4097-a3bf-aa1695843dc3",
            "00020b0d-63c2-4855-892d-3a7340e48848", "00020ede-c38b-4c0e-a94f-f080f635a4f5", "00026d6c-0bd7-482d-a654-a17018af6066"};

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
        ckDemo.setLevel(String.valueOf(random.nextInt(10)));
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
