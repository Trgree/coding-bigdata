package org.ace.spark.test;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Liangsj on 2018/3/8.
 */
public class WriteDB {
    public static void main(String[] args) {
        if(args.length !=4) {
            System.err.println("wrong args! usage: <input> <output> <desSep> <desFileType> ");
            return;

        }
        String input=args[0]; // 输入文件目录
        String output=args[1]; // 输出文件目录
        String desSep=args[2];  // 输出文件分隔符
        String desFileType=args[3];// 输出文件格式  text json parquet





        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark WordCount")
                        .master("local")
                .getOrCreate();

        // TODO 获取表所有字段,查询无数据库

        String columns="word,count";
        String[] cols = columns.split(",");

        Dataset<String> ds = spark.read().text(input).as(Encoders.STRING());

        // 计算单词个数
        Dataset<Row> result = ds.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING())
                .filter((FilterFunction<String>) s -> !s.isEmpty())
                .groupBy("value")
                .count()
                .coalesce(1)
                .withColumnRenamed("value", cols[0])
                .withColumnRenamed("count", cols[1]);

        result.show();
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "aotian#17@dmp");
        String jdbc_url = "jdbc:mysql://192.168.5.150:3306/om";

        result.write()
                .mode(SaveMode.Append) // <--- Append to the existing table
                .jdbc(jdbc_url, "test_wordcount", connectionProperties);
    }
}
