package com.jinfeng.clickhouse;

import com.jinfeng.clickhouse.pojo.UserLog;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
public class ClickHouseKafkaProto {
    public static final String broker_list = "localhost:9092";
    //  kafka topic，Flink 程序中需要和这个统一
    public static final String topic = "kafka_proto_example";

    public static void writeToKafka() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        //  key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //  value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        Random random = new Random();

        UserLog.User.Builder builder = UserLog.User.newBuilder();
        builder.setUserId(UUID.randomUUID().toString());
        builder.setLevel(random.nextInt(5));
        builder.setScore(random.nextInt(100));

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timestamp = format.format( new Date());

        builder.setTimestamp(timestamp);
        String[] names = {"wangjinfeng", "wangjf", "jinfeng.wang"};
        builder.setName(names[random.nextInt(3)]);
        UserLog.User user = builder.build();
        //  System.out.println("user ==>> " + user);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        user.writeDelimitedTo(output);
        //  user.writeTo(output);
        //  String string = Base64.getEncoder().encodeToString(output.toByteArray());
        //  byte[] vs = Base64.getDecoder().decode(string);
        //  UserLog.User users = UserLog.User.parseFrom(vs);
        //  System.out.println("users ==>" + users);
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
