package com.example.HbaseDemo.UI;

import com.example.HbaseDemo.Service.HbaseScanService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Xuan
 * @description  hbase读取数据UI层
 * @date 2020-12-10 11:30
 */
@RestController
public class HbaseScanUI {

    @Autowired
    HbaseScanService service;

    @GetMapping("/old")
    public String oldHbaseScan(@RequestParam("rowkey")String rowkey){
        return this.service.oldHbaseScan(rowkey);
    }

    @GetMapping("/new")
    public String newHbaseScan(@RequestParam("rowkey")String rowkey){
        return this.service.newHbaseScan(rowkey);
    }

}
