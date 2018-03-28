package org.ace.test;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Liangsj on 2018/3/5.
 */
public class WordCount {
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
        //        .master("local")
                .getOrCreate();

        Dataset<String> ds = spark.read().text(input).as(Encoders.STRING());

        // 计算单词个数
        ds.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING())
                .filter((FilterFunction<String>) s -> !s.isEmpty())
                .groupBy("value")
                .count()
                .coalesce(1)  //one partition 最终输出为一个文件
                // 指定格式输出
               // .map((MapFunction<Row, String>) row -> row.mkString(desSep), Encoders.STRING())
                .write()
                .format(desFileType)
                .option("header", "false")
                .save(output);


    }
}
