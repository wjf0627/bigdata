package com.jinfeng.common.utils;

import com.datastax.driver.core.*;

/**
 * @package: com.jinfeng.util
 * @author: wangjf
 * @date: 2020/7/14
 * @time: 11:55 上午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class CassandraConnector {
    public Cluster cluster = null;
    public Session session = null;

    public Session connect() {
        String contactPoint = "localhost";
        String keySpace = "rtdmp";

        if (session == null) {
            PoolingOptions poolingOptions = new PoolingOptions().setConnectionsPerHost(HostDistance.REMOTE, 1, 4);

            cluster = Cluster.builder().addContactPoint(contactPoint).withPoolingOptions(poolingOptions)
                    .withCompression(ProtocolOptions.Compression.SNAPPY).build();
            cluster.init();
            for (Host host : cluster.getMetadata().getAllHosts()) {
                System.out.printf("Address: %s, Rack: %s, Datacenter: %s, Tokens: %s\n", host.getAddress(),
                        host.getDatacenter(), host.getRack(), host.getTokens());
            }
            session = cluster.connect(keySpace);
        }
        return session;
    }

    void close() {
        cluster.close();
    }
}
