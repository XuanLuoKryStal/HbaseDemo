package com.example.HbaseDemo.Service;

import org.springframework.stereotype.Component;


public interface HbaseScanService {
    String oldHbaseScan(String rowkey);

    String newHbaseScan(String rowkey);
}
