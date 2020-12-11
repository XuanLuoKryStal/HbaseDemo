package com.example.HbaseDemo.Service;


import java.util.List;
import java.util.Map;


public interface HbaseScanService {
    List<Map<String,String>> oldHbaseScan(String rowkey);

    String newHbaseScan(String rowkey);
}
