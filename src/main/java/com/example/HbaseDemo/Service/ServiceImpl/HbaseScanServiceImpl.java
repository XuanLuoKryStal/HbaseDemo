package com.example.HbaseDemo.Service.ServiceImpl;

import com.example.HbaseDemo.Service.HbaseScanService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HbaseScanServiceImpl implements HbaseScanService {
    @Autowired
    private Configuration configuration;

    private final static String TABLE_NAME="T_EDI_DECLARATION";

    @Override
    public List<Map<String,String>> oldHbaseScan(String rowkey) {
        //1.创建连接
        HTable table = null;
        ResultScanner scanner = null;
        Scan scan = null;
        Connection connection=null;
        try{
           connection= ConnectionFactory.createConnection(configuration);
           table=(HTable)connection.getTable(TableName.valueOf(TABLE_NAME));
           scan=new Scan();
            //2.前缀表达式获取文件
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            PrefixFilter prefixFilter=new PrefixFilter(Bytes.toBytes(rowkey.trim()));
            filterList.addFilter(prefixFilter);
            scan.setFilter(filterList);
            //3.scan获取查询结果并返回
            scanner=table.getScanner(scan);
            List<Map<String,String>> data=new ArrayList<>();
            int count=10;
            for(Result result:scanner){
                if(count>0) {
                    Map<String, String> tmp = new HashMap<>();
                    tmp.put("rowkey", Bytes.toString(result.getRow()));
                    tmp.put("data", result.toString());
                    data.add(tmp);
                    count--;
                }else{
                    break;
                }
            }
            return data;
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(table!=null){
                try{
                    table.close();
                }catch (IOException e){
                   e.printStackTrace();
                }
            }
        }
      return new ArrayList<>();
    }

    @Override
    public String newHbaseScan(String rowkey) {
        //1.单例获取连接
        //2.范围查询
        //3.scan缓存查询结果
        return null;
    }
}
