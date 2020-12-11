package com.example.HbaseDemo.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class HbaseConfiguration {

    //读取配置文件参数
    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.port}")
    private String clientPort;

    @Value("${zookeeper.znode.parent}")
    private String znodeParent;


    @Bean
    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration configuration= new org.apache.hadoop.conf.Configuration();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.port", clientPort);
        configuration.set("zookeeper.znode.parent", znodeParent);
        return HBaseConfiguration.create(configuration);
    }
}
