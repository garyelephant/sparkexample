package org.example;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Hello world!
 *
 */
public class SparkStreaming
{
    public static void main( String[] args ){
        JavaSparkContext javaSparkContext = new JavaSparkContext();
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(3));

        JavaReceiverInputDStream<String> dstream = streamingContext.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_2());

        // dstream.foreachRDD(rdd -> {
            // rdd.map()....
        //});
    }
}
