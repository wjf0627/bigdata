package com.jinfeng.flink.example;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.esotericsoftware.kryo.Serializer;
import com.jinfeng.flink.entity.DevicePojo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.orc.OrcTableSource;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;

import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.ProtocolVersion.V4;

/**
 * @package: com.jinfeng.flink.example
 * @author: wangjf
 * @date: 2019-06-25
 * @time: 17:32
 * @emial: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class FlinkCassandraTuple {
    public static KeyspaceMetadata keyspaceMetadata;
    //  public static TupleType tupleType;
    private static CodecRegistry registry = new CodecRegistry();
    private static TupleType tupleType = TupleType.of(V4, registry, varchar(), varchar());

    public static void main(String[] args) throws Exception {
        String schema = "struct<device_id:string,age:string,gender:string,install:array<string>,interest:array<string>,frenquency:array<struct<tag:string,cnt:string>>>";

        //  RowTypeInfo schema
        //  Properties properties = new Properties();

        // set up the batch execution environment
        /*
        properties.setProperty("cassandra.maxRequestsPerConnection", "80");
        properties.setProperty("cassandra.maxConnectionsPerHost", "10");
        properties.setProperty("cassandra.coreConnectionsPerHost", "1");
        properties.setProperty("cassandra.port", "9042");
        properties.setProperty("cassandra.ttl", "1468800");
        properties.setProperty("cassandra.local.host", "127.0.0.1");
         */

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        Configuration configuration = new Configuration();

        String path = "/Users/wangjf/Workspace/data/part-00003.snappy.orc";
        OrcTableSource orcTableSource = OrcTableSource.builder()
                .path(path)
                .forOrcSchema(schema)
                .withConfiguration(configuration)
                .build();

        ClusterBuilder clusterBuilder = new ClusterBuilder() {
            private static final long serialVersionUID = -1754532803757154795L;

            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoints("127.0.0.1").build();
            }
        };
        //  tupleType = clusterBuilder.getCluster().getMetadata().newTupleType(DataType.text(), DataType.text());
        keyspaceMetadata = clusterBuilder.getCluster().getMetadata().getKeyspace("dmp_realtime_service");

        DataSet<DevicePojo> dataSet = orcTableSource.getDataSet(env).map(new ConvertPojo());
        //  .filter(new DeviceFilter());

        dataSet.output(new CassandraPojoOutputFormat<>(clusterBuilder, DevicePojo.class, () -> new Mapper.Option[]{Mapper.Option.saveNullFields(false)}));
        env.execute("write");
    }

    private static final class ConvertPojo extends RichMapFunction<Row, DevicePojo> {
        @Override
        public DevicePojo map(Row row) {
            return new DevicePojo(tupleType, row.getField(0), row.getField(1), row.getField(2), row.getField(3), row.getField(4), Arrays.asList(row.getField(5)));
        }
    }

    public static class DeviceFilter implements FilterFunction<DevicePojo> {
        @Override
        public boolean filter(DevicePojo devicePojo) {

            return devicePojo.getFrequency() != null && devicePojo.getFrequency().size() >= 2;
        }
    }
}
