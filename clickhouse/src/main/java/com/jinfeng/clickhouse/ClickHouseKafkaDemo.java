package com.jinfeng.clickhouse;

import com.jinfeng.clickhouse.pojo.KeyValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
public class ClickHouseKafkaDemo {
    public static final String broker_list = "localhost:9092";
    //  kafka topic，Flink 程序中需要和这个统一
    public static final String topic = "kafka_proto_java";

    public static void writeToKafka() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        //  key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //  value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        Random random = new Random();

        KeyValue.KeyValuePair.Builder builder = KeyValue.KeyValuePair.newBuilder();
        builder.setKey(random.nextInt(10));
        builder.setValue(UUID.randomUUID().toString());
        KeyValue.KeyValuePair keyValuePair = builder.build();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        keyValuePair.writeDelimitedTo(output);
        /*
        String string = Base64.getEncoder().encodeToString(output.toByteArray());
        byte[] vs = Base64.getDecoder().decode(string);
        ByteArrayInputStream input = new ByteArrayInputStream(vs);
        KeyValue.KeyValuePair keyValuePair1 = KeyValue.KeyValuePair.parseDelimitedFrom(input);
        System.out.println("keyValuePair1 ==>" + keyValuePair1);
         */
        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, output.toString());
        producer.send(record);
        System.out.println("发送数据: " + output.toString());

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
