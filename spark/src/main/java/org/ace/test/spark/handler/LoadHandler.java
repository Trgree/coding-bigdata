package org.ace.test.spark.handler;



import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Liangsj on 2018/3/28.
 */
public class LoadHandler implements Handler {
    private String srcSep;

    private String input;

    @Override
    public void setParam(String jsonString) {
        JSONObject obj = JSONObject.parseObject(jsonString);
        input = obj.getString("path");
        srcSep = obj.getString("sep");
    }

    @Override
    public Dataset handle(SparkSession sparkSession, Dataset source) {
        Dataset<Row> result = sparkSession.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("sep", srcSep) // 指定分隔符
                // .option("header", "true")
                .load(input);// 本地文件
        return result;
    }
}
