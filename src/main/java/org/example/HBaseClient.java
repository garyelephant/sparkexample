package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClient {

  static Configuration conf =null;
  private static final String ZKconnect="nn01:2181";
  static{
    conf= HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", ZKconnect);
  }
//  static String tableName="student";
//  static String[] family={"lie01","lie02"};


  public static void main(String[] args) throws Exception {

    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
  }


  public static void printKeyValye(KeyValue kv) {

    System.out.println(
      Bytes.toString(kv.getRow()) + "\t"
        + Bytes.toString(kv.getFamily()) + "\t"
        + Bytes.toString(kv.getQualifier()) + "\t"
        + Bytes.toString(kv.getValue()) + "\t"
        + kv.getTimestamp());
  }

  public static void printCell(Cell cell) {

    System.out.println(Bytes.toString(cell.getRow()) + "\t"
      + Bytes.toString(cell.getFamily()) + "\t"
      + Bytes.toString(cell.getQualifier()) + "\t"
      + Bytes.toString(cell.getValue()) + "\t"
      + cell.getTimestamp());
  }

  /**
   * create table
   * */
  public void createTable(Admin admin, String tableName, String[] family) throws Exception {

    TableName tableNameObj = TableName.valueOf(tableName);
    HTableDescriptor desc =new HTableDescriptor(tableNameObj);

    for(int i=0; i<family.length; i++){

      desc.addFamily(new HColumnDescriptor(family[i]));
      System.out.println("create table family: " + family[i]);
    }

    if(admin.tableExists(tableNameObj)){
      System.out.println("table already exists: " + tableName);

    } else {

      admin.createTable(desc);
      System.out.println("finished creating table: " + tableName);
    }
  }

  /**
   * describe table
   * */
  public void descTable(Admin admin, String tableName) throws Exception {

    HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();

    for(HColumnDescriptor t : columnFamilies){
      System.out.println(Bytes.toString(t.getName()));
    }
  }

  /**
   * list tables
   * */
  public void listTables(Admin admin) throws Exception {

    TableName[] tableNames = admin.listTableNames();
    for(int i=0; i<tableNames.length; i++){

      System.out.println(tableNames[i]);
    }
  }

  /**
   * Put Data
   * */
  public void putData(Connection conn, String tableName, String rowKey, String familyName, String columnName, String value)
    throws Exception {

    Table table = conn.getTable(TableName.valueOf(tableName));

    Put put=new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
    table.put(put);
  }

  /**
   * checkAndPut
   * */
  public void checkAndPutData() {

  }

  /**
   * Append
   * */
  public void appendData() {

  }

  /**
   * Increment
   * 当你想把数据库中的某个列的数字+1时，要怎么做呢？先查出来，然后+1之后再存进去吗？
   * 虽然这么做也是可以的，但是很消耗性能，而且不能保证原子性。实际上HBase专门为此设计了一个方法叫increment
   *
   * 不过在increment操作之前你必须先保证你在HBase中存储的数据是long格式的，而不是字符串格式。
   * */
  public void incrementData(Connection conn, String tableName, String rowKey, String familyName, String columnName, String value, long incAmount)
    throws Exception {

    putData(conn, tableName, rowKey, familyName, columnName, value);

    Increment inc = new Increment(Bytes.toBytes(rowKey));
    inc.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), incAmount);
    Table table = conn.getTable(TableName.valueOf(tableName));
    table.increment(inc);
  }

  /**
   * Find by rowkey
   * */
  public Result getResult(Connection conn, String tableName, String rowKey) throws Exception {

    Table table = conn.getTable(TableName.valueOf(tableName));

    Get get = new Get(Bytes.toBytes(rowKey));
    Result result = table.get(get);

      for(KeyValue k:result.list()){
          System.out.println(Bytes.toString(k.getFamily()));
          System.out.println(Bytes.toString(k.getQualifier()));
          System.out.println(Bytes.toString(k.getValue()));
          System.out.println(k.getTimestamp());
      }
    return result;
  }

  /**
   * Find by rowkey and specify column family and column name
   * */
  public Result getResult(Connection conn, String tableName, String rowKey, String familyName, String columnName) throws Exception {

    Table table = conn.getTable(TableName.valueOf(tableName));

    Get get=new Get(Bytes.toBytes(rowKey));

    get.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName));

    Result result=table.get(get);

    for(KeyValue k:result.list()){
      System.out.println(Bytes.toString(k.getFamily()));
      System.out.println(Bytes.toString(k.getQualifier()));
      System.out.println(Bytes.toString(k.getValue()));
      System.out.println(k.getTimestamp());
    }
    return result;
  }

  /**
   * Batch Ops
   * */
  public void batchData() {

  }

  /**
   * Scan
   * */
  public ResultScanner getResultScann(Connection conn, String tableName) throws Exception {

    Table table = conn.getTable(TableName.valueOf(tableName));

    Scan scan = new Scan();
    ResultScanner rs =null;

    try{
      rs = table.getScanner(scan);
      for(Result r: rs){
        for(KeyValue kv:r.list()){

          System.out.println(Bytes.toString(kv.getRow()));
          System.out.println(Bytes.toString(kv.getFamily()));
          System.out.println(Bytes.toString(kv.getQualifier()));
          System.out.println(Bytes.toString(kv.getValue()));
          System.out.println(kv.getTimestamp());
        }
      }
    } finally {
      rs.close();
    }
    return rs;
  }

  /**
   * Get Cell By version
   * */
  public Result getResultByVersion(Connection conn, String tableName, String rowKey, String familyName, String columnName,
                                   int versions) throws Exception {

    Table table = conn.getTable(TableName.valueOf(tableName));

    Get get =new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
    get.setMaxVersions(versions);
    Result result=table.get(get);

    for(KeyValue kv: result.list()){

      System.out.println(Bytes.toString(kv.getFamily()));
      System.out.println(Bytes.toString(kv.getQualifier()));
      System.out.println(Bytes.toString(kv.getValue()));
      System.out.println(kv.getTimestamp());

    }

    return result;
  }

  /**
   * Delete data
   * */
  public void deleteColumn(Connection conn, String tableName, String rowKey, String falilyName, String columnName) throws Exception {

    Table table = conn.getTable(TableName.valueOf(tableName));

    Delete de =new Delete(Bytes.toBytes(rowKey));
    de.addColumn(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
    table.delete(de);
  }


  /**
   * deleteall
   * */
  public void deleteRow(Connection conn, String tableName, String rowKey) throws Exception {

    Table table = conn.getTable(TableName.valueOf(tableName));

    Delete de = new Delete(Bytes.toBytes(rowKey));
    table.delete(de);
  }

  /**
   * Disable table
   * */
  public void disableTable(Admin admin, String tableName) throws Exception {

    admin.disableTable(TableName.valueOf(tableName));

  }

  /**
   * Drop table
   * */
  public void dropTable(Admin admin, String tableName) throws Exception {

    TableName tableNameObj = TableName.valueOf(tableName);
    admin.disableTable(tableNameObj);
    admin.deleteTable(tableNameObj);
  }
}
