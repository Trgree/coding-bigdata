package org.ace.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Liangsj on 2018/3/8.
 */
public class WriteHdfs {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("javaKMeans")
                .master("local")
                .getOrCreate();

        String resultPath="hdfs://192.168.5.150:8020/tmp/test2/";
        List<String> data = Arrays.asList("aaaa");

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.write().format("csv").option("header", "false").save(resultPath);
        spark.stop();

    }
}
