package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * 在这里演示一个Spark SQL的例子。
 * */
public class SparkSQL {

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
        Dataset<Row> df3 = spark.createDataFrame(rowList, schema);
        df3.printSchema();
        df3.show(false);


        // 用真数据, 无论你有多少数据量。
        // Dataset<Row> dfRealData =
        //   spark.read().csv("/path/to/your/hdfs/files");

        df3.registerTempTable("t2");

        Dataset<Row> df4 =
          spark.sql("select name, (age * 3) as modified_age from t2");
        df4.printSchema();
        df4.show(false);
        df4.registerTempTable("t3");

        Dataset<Row> df5 =
          spark.sql("select avg(age) as avg_age from t2");
        df5.printSchema();
        df5.show(false);

        // Join的数据源完全可以是多源，比如说：一个来自HDFS，另一个来自Elasticsearch,
        // 还可以来自MySQL
        Dataset<Row> df6 =
          spark.sql("select t2.*, t3.modified_age from t2 join t3 on t2.name = t3.name");

        df6.printSchema();
        df6.show(false);

        // 底层的RDD操作。
        JavaRDD<String> myRdd = df6.javaRDD().map(v -> {
            // 获取第一个字段(name)并返回。
            // map函数的逻辑，完全是我们自己写的业务逻辑.
            return v.getString(0);
        });

        // myRdd 直到foreach 这里才真正去执行。
        myRdd.foreach(v -> {
            System.out.println(v);
        });
    }
}
