package org.ace.test.task;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Liangsj on 2018/3/28.
 */
public class FilterHandler implements Handler<Dataset<Row>, Dataset<Row>> {

    @Override
    public Dataset<Row> handle(SparkSession sparkSession, Dataset<Row> source) {
        Dataset result =  source.filter((FilterFunction<Row>) row -> {
            String col1 = null;
            Integer col2 = null;
            try {
                col1 = (String) row.get(0);
                col2 = (Integer) row.get(1);
                return col1.length() > 3 && col2 > 20;
            } catch (Exception e) {
            }
            return false;
        });
        System.out.println("清洗文件完成");
        return result;
    }
}
