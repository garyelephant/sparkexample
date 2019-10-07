package org.example;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

public class SparkRDD {
    public static void main(String[] args) throws Exception {
        SparkSession sparkSS = SparkSession.builder().getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext();
        // JavaSparkContext javaSparkContext = sparkSS.sparkContext();
        RDD<String> rdd1 = sparkSS.sparkContext().textFile("a.txt", 2);

//        RDD<String> rdd2 = rdd1.map(record -> { return record; });
//
//        RDD<String> rdd3 = rdd2.filter(record -> {
//            return record.startsWith("abc");
//        });
//
//        rdd4 = rdd1.flatMap();
//
//        String[] ss = rdd3.collect();
//        for (String s : ss) {
//            System.out.println(s);
//        }

        // javaSparkContext.textFile()


    }
}
