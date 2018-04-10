package org.ace.spark.task.handler.impl;


import com.alibaba.fastjson.JSONObject;
import org.ace.spark.task.handler.AbstractHandler;
import org.ace.spark.task.pojo.DataType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 加载文本数据实现
 * csv json parquet文件加载
 * Created by Liangsj on 2018/3/28.
 */
public class FileLoadHandler extends AbstractHandler {
    protected String srcSep;
    protected String input;
    protected DataType dataType;
    protected JSONObject jsonObj;

    /**
     * 初始化自己的参数
     * 解析paramJson，得到当前结点计算需要的参数
     */
    @Override
    public void initParam() {
        this.jsonObj = JSONObject.parseObject(paramJson);
        input = jsonObj.getString("path");
        srcSep = jsonObj.getString("sep");
        dataType = DataType.valOf(jsonObj.getString("dataType"));
        logger.info("输入结点初始化完成，路径={},文件类型={},分隔符={}", input, dataType.getVal(), srcSep);
    }

    /**
     * 加载数据到Dataset
     *
     * @param sparkSession
     * @return
     */
    @Override
    public Dataset<Row> handle(SparkSession sparkSession) {
        logger.info("加载文件,路径={},文件类型={},分隔符={}",input, dataType.getVal(), srcSep);
        Dataset<Row> result = sparkSession.read()
                .format(dataType.getVal())
                .option("inferSchema", "true")
                .option("sep", srcSep) // 指定分隔符
                // .option("header", "true")
                .load(input);// 本地文件
        logger.info("加载文件结点Dataset生成");
        return result;
    }
}
