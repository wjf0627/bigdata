package com.jinfeng.clickhouse;

import com.google.gson.JsonObject;
import com.jinfeng.clickhouse.pojo.DeviceExample;
import com.jinfeng.common.utils.GsonUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @package: com.jinfeng.clickhouse
 * @author: wangjf
 * @date: 2019-07-05
 * @time: 18:24
 * @emial: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class ClickHouseFlinkSink {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static final ArrayList<DeviceExample> deviceExamples = new ArrayList<>(1000);

    static {
        for (int i = 0; i < 100000; i++) {
            deviceExamples.add(new DeviceExample("" + UUID.randomUUID(), sdf.format(new Date()), new Random().nextInt(10)));
        }
    }

    private static Map<String, String> buildGlobalParameters() {
        Map<String, String> globalParameters = new HashMap<>();

        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "http://localhost:8123");
        globalParameters.put(ClickhouseSinkConsts.TIMEOUT_SEC, "3");
        globalParameters.put(ClickhouseSinkConsts.FAILED_RECORDS_PATH, "/Users/wangjf/Workspace/Project/wangjf/clickhouse/tmp/failed_records");
        globalParameters.put(ClickhouseSinkConsts.NUM_WRITERS, "5");
        globalParameters.put(ClickhouseSinkConsts.NUM_RETRIES, "10");
        globalParameters.put(ClickhouseSinkConsts.QUEUE_MAX_CAPACITY, "1000");

        return globalParameters;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        ParameterTool parameters = ParameterTool.fromMap(buildGlobalParameters());
        environment.getConfig().setGlobalJobParameters(parameters);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        Properties props = new Properties();
        props.put(ClickhouseSinkConsts.TARGET_TABLE_NAME, "wangjf.flink_test");
        props.put(ClickhouseSinkConsts.MAX_BUFFER_SIZE, "100");

        DataStream<String> dataStream = environment.addSource(new FlinkKafkaConsumer<>("kafka_topic", new SimpleStringSchema(), properties));
        dataStream.map(new toClickHouseInsertFormatByJSON())
                .addSink(new ClickhouseSink(props));
        //  build chain
        /*
        DataStream<DeviceExample> dataStream = environment.fromCollection(deviceExamples);
        dataStream.map(new toClickHouseInsertFormat())
                .addSink(new ClickhouseSink(props));
        */

        environment.execute("wangjf.flink_test clickhouse sink");
    }

    private static final class toClickHouseInsertFormat extends RichMapFunction<DeviceExample, String> {
        @Override
        public String map(DeviceExample deviceExample) {
            StringBuilder builder = new StringBuilder();
            builder.append("(");

            //  device_id
            builder.append("'");
            builder.append(deviceExample.getDevice_id());
            builder.append("', ");

            builder.append("'");
            builder.append(deviceExample.getDt());
            builder.append("', ");

            //  flag
            builder.append(deviceExample.getFlag());
            builder.append(")");

            //  System.out.println("Message -->> " + builder.toString());
            return builder.toString();
        }
    }

    private static final class toClickHouseInsertFormatByJSON extends RichMapFunction<String, String> {
        @Override
        public String map(String str) {
            JsonObject jsonObject = GsonUtil.String2JsonObject(str);
            StringBuilder builder = new StringBuilder();
            builder.append("(");

            //  device_id
            builder.append("'");
            builder.append(jsonObject.get("message").getAsString());
            builder.append("', ");

            builder.append("'");
            String dt = new SimpleDateFormat("yyyy-MM-dd").format(new Date(jsonObject.get("timestamp").getAsLong()));
            builder.append(dt);
            builder.append("', ");

            builder.append("'");
            String time = new SimpleDateFormat("mm").format(new Date(jsonObject.get("timestamp").getAsLong()));
            builder.append(time);
            builder.append("', ");

            //  flag
            builder.append(jsonObject.get("level").getAsInt());
            builder.append(")");

            System.out.println("Message -->> " + builder.toString());
            return builder.toString();
        }
    }
}
