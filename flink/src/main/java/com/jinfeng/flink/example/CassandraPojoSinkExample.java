package com.jinfeng.flink.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import com.jinfeng.flink.entity.Message;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.ArrayList;
import java.util.UUID;

/**
 * @package: com.jinfeng.flink
 * @author: wangjf
 * @date: 2019/3/28
 * @time: 下午5:50
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class CassandraPojoSinkExample {
    private static final ArrayList<Message> messages = new ArrayList<>(20);

    static {
        for (long i = 1; i < 100; i++) {
            messages.add(new Message("" + UUID.randomUUID()));
        }
    }

    public static void main(String[] args) throws Exception {
        long a = System.currentTimeMillis();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Message> source = env.fromCollection(messages);

        CassandraSink.addSink(source)
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                })
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build();

        env.execute("Cassandra Sink example");
        long b = System.currentTimeMillis();
        System.out.println("Flink Run Time == " + (b - a));
    }
}
