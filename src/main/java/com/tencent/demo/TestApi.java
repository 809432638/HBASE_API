package com.tencent.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * @Author 嫌疑人:杨某
 * @Date 2019/11/09 10:24
 * @Version 1.0
 */
public class TestApi {

    //声明连接
    private static Connection connection =null;
    private static Admin admin=null;

    //初始化
    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

  //释放资源
  private static void close() throws IOException {
        if (connection != null){
            connection.close();
        }
        if (admin != null){
            admin.close();
        }
  }

  //判断表是否存在
    public static boolean isTableExsit(String tableName) throws IOException {
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        return  tableExists;
    }


    //创建表
    public  static void createTable(String tableName,String... cf) throws IOException {
        if (isTableExsit(tableName)){
            System.out.println("表已经存在");
            return;
        }
        if (cf.length<=0){
            System.out.println("参数异常");
        }
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //遍历列族
        for (String s : cf) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(s);
            descriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(descriptor);
    }


    //删除表
    public  static void deleteTable(String tableName) throws IOException {
        if (!isTableExsit(tableName)){
            System.out.println("表不存在");
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }


    //创建命名空间
    public static void createNameSpace(String nameSpace)  {
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace).build();
        try {
            admin.createNamespace(descriptor);
        } catch (NamespaceExistException e) {
            System.out.println(nameSpace + "此命名空间已经存在");
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    //添加数据
    public static void put(String tableName,String rk,String cf,String cn,String val) throws IOException {
           //创建表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 声明put对象
        Put putobj = new Put(Bytes.toBytes(rk));
        // 向put对象中添加值
        putobj.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(val));
        //向表中添加数据
        table.put(putobj);
        // 关闭
        table.close();
    }


    //查询数据 get
     public static void get(String tableName,String rk) throws IOException {
        // 1. 获取对象
         Table table = connection.getTable(TableName.valueOf(tableName));
         // 2. 获得get对象
         Get getobj = new Get(Bytes.toBytes(rk));
         // 3. 获取查询结果
         Result result = table.get(getobj);
         // 4. 遍历获取单个cell并打印
         for (Cell cell : result.rawCells()) {
             System.out.println("行健:"+Bytes.toString(CellUtil.cloneRow(cell))+
             "列族:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                     "列:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                     "值:"+Bytes.toString(CellUtil.cloneValue(cell))
             );
         }
         // 5. 释放资源
         table.close();
     }


      //查询数据 ---> scan
      public static void scan(String tableName) throws IOException {
          // 1. 获取表对象
          Table table = connection.getTable(TableName.valueOf(tableName));
          // 2. 创建Scan对象单个行健遍历
         // Scan scanobj = new Scan(Bytes.toBytes("10001"));
          // 2. 创建Scan对象多个行健遍历
          Scan scanobj = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1003"));
          // 3. 查询数据并获取结果
          ResultScanner results = table.getScanner(scanobj);
          // 4. 单个行健遍历
        /*  for (Cell cell : results.next().rawCells()) {
              System.out.println("行健:"+Bytes.toString(CellUtil.cloneRow(cell))+
                      "列族:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                      "列:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                      "值:"+Bytes.toString(CellUtil.cloneValue(cell)));
          }*/
          //4. 多个行健遍历
          for (Result result : results) {
              for (Cell cell : result.rawCells()) {
                  System.out.println("行健:"+Bytes.toString(CellUtil.cloneRow(cell))+
                          "列族:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                          "列:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                          "值:"+Bytes.toString(CellUtil.cloneValue(cell)));
              }
          }
          // 5. 释放资源
          table.close();
      }


      //删除数据 ---> delete
      public static void delete(String tableName, String rk) throws IOException {
          // 1. 获取表对象
          Table table = connection.getTable(TableName.valueOf(tableName));
          // 2. 执行删除逻辑
          Delete deleteobj = new Delete(Bytes.toBytes(rk));
          table.delete(deleteobj);
          // 3. 释放资源
          table.close();
      }
    public static void main(String[] args) throws IOException {
        //测试表是否存在
        //System.out.println(isTableExsit("user"));

        // 创建表
       // createTable("stu","info","name","da","ss");

        //删除表
        //deleteTable("user");

        // 创建命名空间
        //createNameSpace("1703D");

        //. 插入数据
        //put("user","1002","info","kobe","vip");

        //插入两个列族
       // put("stu","1001","info","sex","nan");
        //put("stu","1001","da","cc","vv");

         //查询数据 get
        //get("stu","1001");

        //查询数据 ---> scan
        // scan("user");

        //删除数据 ---> delete
       // delete("user","1001");

    }

}
