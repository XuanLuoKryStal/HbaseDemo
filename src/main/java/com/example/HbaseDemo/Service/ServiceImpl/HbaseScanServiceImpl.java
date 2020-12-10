package com.example.HbaseDemo.Service.ServiceImpl;

import com.example.HbaseDemo.Service.HbaseScanService;
import org.springframework.stereotype.Service;

@Service
public class HbaseScanServiceImpl implements HbaseScanService {
    @Override
    public String oldHbaseScan(String rowkey) {
        return "success";
    }

    @Override
    public String newHbaseScan(String rowkey) {
        return null;
    }
}
