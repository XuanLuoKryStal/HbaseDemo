package com.example.HbaseDemo.UI;

import com.example.HbaseDemo.Service.HbaseScanService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author Xuan
 * @description  hbase读取数据UI层
 * @date 2020-12-10 11:30
 */
@RestController
public class HbaseScanUI {

    @Autowired
    HbaseScanService service;

    //01160610211100x500000960xxxxx8247804
    private static String ROWKEY="01160610211100x500000960xxxxx82478";

    @GetMapping("/old")
    public List<Map<String,String>> oldHbaseScan(){
        String rowkey=ROWKEY;
        return this.service.oldHbaseScan(rowkey);
    }

    @GetMapping("/new")
    public List<Map<String,String>> newHbaseScan(){
        String rowkey=ROWKEY;
        return this.service.newHbaseScan(rowkey);
    }

}
