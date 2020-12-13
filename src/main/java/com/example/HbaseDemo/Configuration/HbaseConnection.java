package com.example.HbaseDemo.Configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class HbaseConnection {

    @Autowired
    private Configuration configuration;

    private volatile Connection instance;

    public  Connection getInstance() {
        if (instance == null) {
            try {
                synchronized (this) {
                    if(instance==null) {
                        this.instance = ConnectionFactory.createConnection(configuration);
                    }
                    return instance;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this.instance;
    }

}
