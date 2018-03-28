package org.ace.test.spark.handler;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Liangsj on 2018/3/16.
 */
public class SaveHandler implements Handler {

    private String desSep;

    private String output;


    @Override
    public void setParam(String jsonString) {
        JSONObject obj = JSONObject.parseObject(jsonString);
        desSep = obj.getString("sep");
        output = obj.getString("path");
    }

    @Override
    public Dataset handle(SparkSession sparkSession, Dataset dataset) {
        dataset.write()
                .format("csv")
                .option("header", "false")
                .option("sep",desSep)
                .save(output);
        System.out.println("文件输出完成");
        return null;
    }
}
