package org.ace.spark.test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Function1;
import scala.collection.TraversableOnce;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 计算单词个数
 * Created by Liangsj on 2018/2/27.
 */
public class Wordcount_Dataset {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();
        Dataset<String> ds = spark.read()
      //          .text("hdfs://192.168.5.150:8020/tmp/sparktest/people.txt")
               .text("spark/file/file01.txt")// 本地文件
                .as(Encoders.STRING());

        Dataset<String> words = ds.flatMap(
                (FlatMapFunction<String, String>) s ->
                {
                    return Arrays.asList(s.split(" ")).iterator();
                }, Encoders.STRING())
                .filter((FilterFunction<String>)s -> !s.isEmpty())
                .coalesce(1);//one partition (parallelism level)

      //  words.show();

        Dataset<Row> t = words.groupBy("value") //<k, iter(V)>
                .count()
                .toDF("word", "count");
        t = t.sort(functions.desc("count"));
        t.show();

    }
}
