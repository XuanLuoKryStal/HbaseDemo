package com.example.HbaseDemo.Service.ServiceImpl;

import com.example.HbaseDemo.Configuration.HbaseConnection;
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
    @Autowired
    private HbaseConnection hbaseConnection;

    private final static String TABLE_NAME="T_EDI_GOODS";

    @Override
    public List<Map<String,String>> oldHbaseScan(String rowkey) {
        //1.创建连接
        HTable table = null;
        Scan scan = null;
        Connection connection=null;
        try{
            Long start = System.currentTimeMillis();
           connection= ConnectionFactory.createConnection(configuration);
            Long end = System.currentTimeMillis();
            System.out.println("old search "+(end-start)+"ms");
           System.out.println("Thread=="+Thread.currentThread().getName()+"==="+connection);
           table=(HTable)connection.getTable(TableName.valueOf(TABLE_NAME));
           scan=new Scan();
            //2.前缀表达式获取文件
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            PrefixFilter prefixFilter=new PrefixFilter(Bytes.toBytes(rowkey.trim()));
            filterList.addFilter(prefixFilter);
            scan.setFilter(filterList);
            //3.scan获取查询结果并返回
            return this.getResult(table,scan);
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

    private List<Map<String, String>> getResult(HTable table, Scan scan) throws IOException {
        scan.addColumn(Bytes.toBytes("E"),
                Bytes.toBytes("GN"));
        ResultScanner scanner = null;
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
        try {
            Thread.sleep(10);
        }catch (Exception e){
            e.printStackTrace();
        }
        return data;
    }

    @Override
    public List<Map<String,String>> newHbaseScan(String rowkey) {
        //1.单例获取连接
        Long start = System.currentTimeMillis();
        Connection connection=hbaseConnection.getInstance();
        System.out.println("Thread=="+Thread.currentThread().getName()+"==="+connection);
        //2.范围查询
        HTable table = null;
        Scan scan = null;
        try{
            table=(HTable)connection.getTable(TableName.valueOf(TABLE_NAME));
            Long end = System.currentTimeMillis();
            System.out.println("new search "+(end-start)+"ms");
            scan=new Scan();
            scan.setBatch(100);
            scan.setCaching(100);
            scan.setMaxResultSize(100);
            String startRow=rowkey+"00";
            String stopRow=rowkey+"xx";
            //3.scan缓存查询结果
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            return this.getResult(table,scan);
        }catch (IOException e){
            e.printStackTrace();
        }
        return new ArrayList<>();
    }
}
