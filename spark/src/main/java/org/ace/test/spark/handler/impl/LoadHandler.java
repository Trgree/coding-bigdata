package org.ace.test.spark.handler.impl;



import com.alibaba.fastjson.JSONObject;
import org.ace.test.spark.DatasetStorage;
import org.ace.test.spark.handler.AbstractHandler;
import org.ace.test.spark.pojo.Node;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 加载数据实现
 * Created by Liangsj on 2018/3/28.
 */
public class LoadHandler extends AbstractHandler {
    private String srcSep;

    private String input;

    /**
     * 初始化自己的参数
     */
    @Override
    public void initParam() {
        JSONObject obj = JSONObject.parseObject(paramJson);
        input = obj.getString("path");
        srcSep = obj.getString("sep");
    }

    /**
     * 加载数据到Dataset
     * @param sparkSession
     * @return
     */
    @Override
    public Dataset<Row> handle(SparkSession sparkSession) {
        Dataset<Row> result = sparkSession.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("sep", srcSep) // 指定分隔符
                // .option("header", "true")
                .load(input);// 本地文件
        System.out.println("加载文件完成");
        return result;
    }
}
