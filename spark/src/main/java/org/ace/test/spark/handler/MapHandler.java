package org.ace.test.spark.handler;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Liangsj on 2018/3/16.
 */
public class MapHandler implements Handler {

    private String desSep;

    @Override
    public void setParam(String jsonString) {
        JSONObject obj = JSONObject.parseObject(jsonString);
        desSep = obj.getString("sep");
    }

    @Override
    public Dataset handle(SparkSession sparkSession, Dataset dataset) {
        Dataset result =  dataset
                // 指定格式输出
                .map((MapFunction<Row, String>) row -> {
//                    TimeUnit.SECONDS.sleep(10);
                    System.out.println("map");
                    return row.mkString(desSep);
                }, Encoders.STRING());
        System.out.println("文件格式完成");
        return result;
    }
}
