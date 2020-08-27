package com.jinfeng.flink.utils;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.Mapper;
import com.jinfeng.flink.entity.ReflectiveDeviceCase;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;

import java.util.Properties;

/**
 * @author wangjf
 */
public class CommenCassandraSinkOrc extends CassandraPojoOutputFormat<ReflectiveDeviceCase> {

    public CommenCassandraSinkOrc(Properties properties, String region) {
        super(new CassandraClusterBuilder(properties, region),
                ReflectiveDeviceCase.class,
                (MapperOptions) () -> new Mapper.Option[]{Mapper.Option.saveNullFields(true),
                        Mapper.Option.ttl(Integer.parseInt(properties.getProperty("cassandra.ttl"))),
                        Mapper.Option.consistencyLevel(ConsistencyLevel.ONE)});
    }
}