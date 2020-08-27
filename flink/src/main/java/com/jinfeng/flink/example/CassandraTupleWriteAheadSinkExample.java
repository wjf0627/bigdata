package com.jinfeng.flink.example;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * @package: com.jinfeng.flink
 * @author: wangjf
 * @date: 2019/3/28
 * @time: 下午10:27
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class CassandraTupleWriteAheadSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000));
        env.setStateBackend(new FsStateBackend("file:///" + System.getProperty("java.io.tmpdir") + "/flink/backend"));

        CassandraSink<Tuple2<String, Integer>> sink = CassandraSink.addSink(env.addSource(new MySource()))
                .setQuery("INSERT INTO example.values(id, count) values (?, ?);")
                .enableWriteAheadLog()
                .setClusterBuilder(new ClusterBuilder() {

                    private static final long serialVersionUID = 2793938419775311824L;

                    @Override
                    public Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                })
                .build();

        sink.name("Cassandra Sink").disableChaining().setParallelism(1).uid("hello");

        env.execute();
    }

    private static class MySource implements SourceFunction<Tuple2<String, Integer>>, ListCheckpointed<Integer> {
        private static final long serialVersionUID = 4022367939215095610L;

        private int counter = 0;
        private boolean stop = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {

            for (int i = 1; i < 10; i++) {
                ctx.collect(new Tuple2<>("" + UUID.randomUUID(), i));
            }
        }

        @Override
        public void cancel() {
            stop = true;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.counter);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
            }
            this.counter = state.get(0);
        }
    }
}
