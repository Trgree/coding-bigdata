package org.ace.test;

import org.apache.spark.sql.SparkSession;

/**
 * Created by Liangsj on 2018/3/2.
 */
public class SessionTest {
    public static void main(String[] args) {
        SparkSession spark2 = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
              //  .master("local")
                .getOrCreate();

        System.out.println("spark2" + spark2);
    }
}
