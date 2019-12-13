package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class SparkParquet {

  public static void main(String[] args) throws Exception {

    // 我在本地以Local[2]模式启动一个spark程序。
    SparkSession spark = SparkSession.builder().getOrCreate();


    // 创建一个假数据。
    // Row 是Spark SQL 里面核心的数据结构:
    Row row1 = RowFactory.create("gary", 28);
    Row row2 = RowFactory.create("Bob", 19);
    Row row3 = RowFactory.create("Jerry", 25);
    List<Row> rowList = Arrays.asList(row1, row2, row3);

    // 用 StructType 定义关于这些Row的Schema
    StructType schema = new StructType()
      .add("name", DataTypes.StringType)
      .add("age", DataTypes.IntegerType);

    // 使用数据，和schema创建 Dataset<Row>
    // DataFrame 可以立即为是一个结构化的数据，也就是一张表。
    Dataset<Row> df1 = spark.createDataFrame(rowList, schema);
    df1.printSchema();
    df1.show(false);

    // 写parquet，不分区
    df1.write().parquet("/analysis/wechat/report1");

    // 写parquet，有分区
    df1.write().partitionBy("name").parquet("/analysis/wechat/report2");

    // 读parquet

    Dataset<Row> df2 = spark.read().parquet("/analysis/wechat/report1");

    df2.printSchema();
    df2.show(false);
  }
}
