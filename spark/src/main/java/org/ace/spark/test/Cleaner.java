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

/**
 * 清洗
 * Created by Liangsj on 2018/3/1.
 */
public class Cleaner {
    public static void main(String[] args) {

        String srcSep = ",";
        String desSep = "|";
        String input = "spark/file/dpi.txt";
        String output = "spark/file/out";
        /*if(args.length !=4) {
            System.err.println("wrong args! usage: <srcSep> <desSep> <input> <output> ");
            return;

        }
        String srcSep=args[0];
        String desSep=args[1];
        String input=args[2];
        String output=args[3];*/

        delLocalDir(output);

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();
        System.out.println("spark1" + spark);

        // 读取文件，并分隔
        Dataset<Row> ds = spark.read()
               .format("csv")
                .option("inferSchema", "true")
                .option("sep", srcSep) // 指定分隔符
                // .option("header", "true")
                .load(input)// 本地文件
              //  .text(input)
        ;
        ds = ds.withColumnRenamed("_c0","c1").withColumnRenamed("_c1","c2");
        ds = ds.drop("c2");
        ds.show();// 数据已分列
        spark.close();


        // 数据清洗
      /*  ds.filter((FilterFunction<Row>) row -> {

            String col1 = null;
            Integer col2 = null;
            try {
                col1 = (String) row.get(0);
                col2 = (Integer) row.get(1);
                return col1.length() > 3 && col2 > 20;
            } catch (Exception e) {
            }
            return false;
        })
            // 指定格式输出
            .map((MapFunction<Row, String>) row -> {
                return row.mkString(desSep);
    }, Encoders.STRING())
            .write()
            .format("text")
            .option("header", "false")
                .option("sep",desSep)
            .save(output);
*/
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
