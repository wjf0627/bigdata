package com.jinfeng.clickhouse;

import com.jinfeng.clickhouse.pojo.DeviceExample;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;

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

    private static final ArrayList<DeviceExample> deviceExamples = new ArrayList<>(1000);

    static {
        for (long i = 0; i < 100000; i++) {
            deviceExamples.add(new DeviceExample("" + UUID.randomUUID(), new Date(), new Random().nextInt(10)));
        }
    }

    private static Map<String, String> buildGlobalParameters() {
        Map<String, String> globalParameters = new HashMap<>();

        //  clickhouse cluster properties
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "http://localhost:8123");
        //  globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_USER, "");
        //  globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, "");

        //  sink common
        globalParameters.put(ClickhouseSinkConsts.TIMEOUT_SEC, "3");
        globalParameters.put(ClickhouseSinkConsts.FAILED_RECORDS_PATH, "/Users/wangjf/Workspace/Project/wangjf/clickhouse/tmp/failed_records");
        globalParameters.put(ClickhouseSinkConsts.NUM_WRITERS, "5");
        globalParameters.put(ClickhouseSinkConsts.NUM_RETRIES, "10");
        globalParameters.put(ClickhouseSinkConsts.QUEUE_MAX_CAPACITY, "1000");

        return globalParameters;
    }

    public static void main(String[] args) throws Exception {
        //  ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        //  set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(buildGlobalParameters());
        environment.getConfig().setGlobalJobParameters(parameters);

        // create props for sink
        Properties props = new Properties();
        props.put(ClickhouseSinkConsts.TARGET_TABLE_NAME, "wangjf.flink_test");
        props.put(ClickhouseSinkConsts.MAX_BUFFER_SIZE, "100000");

        // build chain
        DataStream<DeviceExample> dataStream = environment.fromCollection(deviceExamples);
        dataStream.map(new toClickHouseInsertFormat())
                .addSink(new ClickhouseSink(props));

        environment.execute("wangjf.flink_test clickhouse sink");
    }

    private static final class toClickHouseInsertFormat extends RichMapFunction<DeviceExample, String> {
        @Override
        public String map(DeviceExample deviceExample) {
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            //  dt
            //  builder.append("'");
            //  builder.append("2019-07-07");
            //  builder.append("', ");

            //  device_id
            builder.append("'");
            builder.append(deviceExample.getDevice_id());
            builder.append("', ");

            //  flag
            builder.append(deviceExample.getFlag());
            builder.append(")");

            return builder.toString();
        }
    }
}
