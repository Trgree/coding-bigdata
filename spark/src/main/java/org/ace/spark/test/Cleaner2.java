package org.ace.spark.test;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Liangsj on 2018/3/16.
 */
public class Cleaner2 {
    public static void main(String[] args) {

        String srcSep = ",";
        String desSep = "|";
        String input = "spark/file/dpi.txt";
        String output = "spark/file/out";

        delLocalDir(output);

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();




        // 读取文件，并分隔
        Dataset<Row> ds = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("sep", srcSep) // 指定分隔符
                // .option("header", "true")
                .load(input)// 本地文件
                //  .text(input)
                ;
        //ds.show();// 数据已分列
     //   ds.write().csv(output+"_1");
        System.out.println("读取文件完成");


        Dataset<Row> ds2 =  ds.filter((FilterFunction<Row>) row -> {
            String col1 = null;
            Integer col2 = null;
            try {
                col1 = (String) row.get(0);
                col2 = (Integer) row.get(1);
                TimeUnit.MICROSECONDS.sleep(300);
                System.out.println("=====");
                return col1.length() > 3 && col2 > 20;
            } catch (Exception e) {
            }
            return false;
        });
        ds2.write().csv(output+"_2");
        System.out.println("清洗文件完成");
        ds2.persist();

        Dataset result =  ds2
                // 指定格式输出
                .map((MapFunction<Row, String>) row -> {
                    TimeUnit.SECONDS.sleep(1);
                    System.out.println("-----");
                    return row.mkString(desSep);
                }, Encoders.STRING());
        System.out.println("文件格式完成");

        result.write().csv(output);

        System.out.println("done");

    }

    private static void delLocalDir(String path){
        if(path !=null && !path.endsWith("hdfs")) {
            try {
                FileUtils.deleteDirectory(new File(path));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
