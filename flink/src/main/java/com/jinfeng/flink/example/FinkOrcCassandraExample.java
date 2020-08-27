package com.jinfeng.flink.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.mapping.Mapper;
import com.jinfeng.flink.entity.ReflectiveDeviceCase;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.orc.OrcTableSource;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Properties;

public class FinkOrcCassandraExample {
    public static TupleType tupleType;

    public static void main(String[] args) throws Exception {
        String schema = "struct<device_id:string,age:string,gender:string,install:array<string>,interest:array<string>,frenquency:array<struct<tag:string,cnt:string>>>";

        //  RowTypeInfo schema = new RowTypeInfo(new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO}, new String[]{"tag","cnt"});
        Properties properties = new Properties();

        // set up the batch execution environment
        properties.setProperty("cassandra.maxRequestsPerConnection", "80");
        properties.setProperty("cassandra.maxConnectionsPerHost", "10");
        properties.setProperty("cassandra.coreConnectionsPerHost", "2");
        properties.setProperty("cassandra.port", "9042");
        properties.setProperty("cassandra.ttl", "1468800");
        properties.setProperty("cassandra.local.host", "127.0.0.1");

        //  ReflectiveDeviceCase reflectiveDeviceCase = new ReflectiveDeviceCase();
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        //  BatchTableEnvironment tabEnv = TableEnvironment.getTableEnvironment(env);

        Configuration configuration = new Configuration();

        String path = "/Users/wangjf/Workspace/data/part-00003.snappy.orc";
        OrcTableSource orcTableSource = OrcTableSource.builder()
                .path(path)
                .forOrcSchema(schema)
                .withConfiguration(configuration)
                .build();

        //  tabEnv.registerTableSource("realtime", orcTableSource);
        //  String sql = "SELECT device_id,install FROM realtime";
        //  Table table = tabEnv.sqlQuery(sql);
        //  System.out.println(orcTableSource.getDataSet(env));
        //  DataSet<org.apache.flink.types.Row> dataSet = orcTableSource.getDataSet(env);
        ClusterBuilder clusterBuilder = new ClusterBuilder() {
            private static final long serialVersionUID = -1754532803757154795L;

            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoints("127.0.0.1").build();
            }
        };

        tupleType = clusterBuilder.getCluster().getMetadata().newTupleType(DataType.text(), DataType.text());
        DataSet<ReflectiveDeviceCase> dataSet = orcTableSource.getDataSet(env).map(new ConvertToReflective());
        //  System.out.println(dataSet1.count());
        //  DataSet<ReflectiveDeviceCase> dataSet = tabEnv.toDataSet(table, ReflectiveDeviceCase.class);

        //  DataSet<DeviceCase> dataSet = tabEnv.toDataSet(table, DeviceCase.class);
        //  System.out.println(dataSet1);

        /*
        String insert = "INSERT INTO dmp_realtime_service.dmp_user_features4 (device_id,age,gender,install_apps,interest,frequency) values (?, ?, ?, ?, ?, ?)";
        CassandraRowOutputFormat sink = new CassandraRowOutputFormat(insert, new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                builder.addContactPoint("127.0.0.1");
                return builder.build();
            }
        });
         */

        dataSet.output(new CassandraPojoOutputFormat<>(clusterBuilder, ReflectiveDeviceCase.class, () -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)}));
        //  dataSet.output(sink);
        env.execute("write");

        //  DataSet<ReflectiveDeviceCase> dataSet = tabEnv.toDataSet(table, ReflectiveDeviceCase.class);
        //  DataStream<ReflectiveDeviceCase> streamSource = streamTableEnvironment.toAppendStream(table, ReflectiveDeviceCase.class);

        /*
        CassandraSink.addSink(streamSource)
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                })
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build();
         */


        //  Table table = tabEnv.sqlQuery(sql);
        //  var dataset = env.readTextFile(path)

        //  DataSet<ReflectiveDeviceCase> dataSet = tabEnv.toDataSet(table, ReflectiveDeviceCase.class);
        //  DataSet<Row> dataset = orcTableSource.getDataSet(env);

        //  tabEnv.registerDataSet("");
        //  DataSet<ReflectiveDevice> result = dataset.map(new ConvertToReflective());

        //  dataSet.count();
        //  CommenCassandraSinkOrc commenCassandraSinkOrc = new CommenCassandraSinkOrc(properties, "local");

        //  dataSet1.output(commenCassandraSinkOrc);

        //  env.execute("");
        //  dataSet.output(commenCassandraSinkOrc);
        //  if (!region.contains("virginia")) {
        //  dataset = dataset.rebalance()
        //  }
        //  result
        //  result

        //  System.out.println(result);
    }

    private static final class ConvertToReflective extends RichMapFunction<Row, ReflectiveDeviceCase> {
        @Override
        public ReflectiveDeviceCase map(Row row) {
            //  row.getField(1), row.getField(2),
            return new ReflectiveDeviceCase(tupleType, row.getField(0), Arrays.asList(row.getField(4)));
        }
    }

}
