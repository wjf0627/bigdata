package com.jinfeng.flink.utils;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.Properties;

public class CassandraClusterBuilder extends ClusterBuilder {
    private String[] address;

    private int port;

    private Properties config;

    private int maxRequestsPerConnection;

    private int maxConnectionsPerHost;

    private int coreConnectionsPerHost;

    public CassandraClusterBuilder(Properties properties, String region) {
        this.config = properties;
        this.port = Integer.parseInt(config.getProperty("cassandra.port"));
        this.address = config.getProperty("cassandra." + region + ".host").split(",");
        maxRequestsPerConnection = Integer.parseInt(config.getProperty("cassandra.maxRequestsPerConnection"));
        maxConnectionsPerHost = Integer.parseInt(config.getProperty("cassandra.maxConnectionsPerHost"));
        coreConnectionsPerHost = Integer.parseInt(config.getProperty("cassandra.coreConnectionsPerHost"));
    }

    public String[] getAddress() {
        return address;
    }

    public void setAddress(String[] address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Properties getConfig() {
        return config;
    }

    public void setConfig(Properties config) {
        this.config = config;
    }

    public int getMaxRequestsPerConnection() {
        return maxRequestsPerConnection;
    }

    public void setMaxRequestsPerConnection(int maxRequestsPerConnection) {
        this.maxRequestsPerConnection = maxRequestsPerConnection;
    }

    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public int getCoreConnectionsPerHost() {
        return coreConnectionsPerHost;
    }

    public void setCoreConnectionsPerHost(int coreConnectionsPerHost) {
        this.coreConnectionsPerHost = coreConnectionsPerHost;
    }

    @Override
    protected Cluster buildCluster(Cluster.Builder builder) {
        PoolingOptions poolingOpts = new PoolingOptions();
        poolingOpts.setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestsPerConnection);
        poolingOpts.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHost);
        poolingOpts.setIdleTimeoutSeconds(60);
        poolingOpts.setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnectionsPerHost);
        poolingOpts.setHeartbeatIntervalSeconds(60 * 3);
        poolingOpts.setPoolTimeoutMillis(1000 * 60 * 3);
        return builder.withPoolingOptions(poolingOpts)
                .addContactPoints(address)
                .withPort(port)
                .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(30 * 1000L))
                .withSocketOptions(new SocketOptions()
                        .setTcpNoDelay(true)
                        .setConnectTimeoutMillis(60 * 1000)
                        .setReadTimeoutMillis(120000))
                //  .withCredentials("cassandra", "cassandra")
                .withCompression(ProtocolOptions.Compression.SNAPPY)
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .build();
    }
}
