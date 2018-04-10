package org.ace.spark.task.handler.impl;

import com.alibaba.fastjson.JSONObject;
import org.ace.spark.task.handler.AbstractHandler;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 过滤数据实现
 * Created by Liangsj on 2018/3/16.
 */
public class FilterHandler extends AbstractHandler {

    private String filter;// 元子参数
    private String other;//

    /**
     * 初始化结点的参数
     * 解析paramJson，得到当前结点计算需要的参数
     */
    @Override
    public void initParam() {
        JSONObject obj = JSONObject.parseObject(paramJson);
        filter = obj.getString("filter");// 每个结点，在task-web-job中封装的参数
        if (filter == null) {
            throw new RuntimeException("获取不到filter参数");
        }
        other = obj.getString("xxx");// 其它参数
    }

    /**
     * 过滤包含filter字符的行
     *
     * @param sparkSession
     * @return 过滤后的Dataset
     */
    @Override
    public Dataset<Row> handle(SparkSession sparkSession) {
        if (sourceDataset == null) {
            throw new RuntimeException(currentSparkNode.getName() + "无上结点或无输出");
        }
        logger.info("过滤行结点Dataset定义");
        Dataset<Row> result = sourceDataset.filter((FilterFunction<Row>) row -> {
            for (int i = 0; i < row.length(); i++) {
                String c = row.get(i) + "";
                if (c.contains(filter)) {
                    return false;
                }
            }
            return true;
        });

        logger.info("过滤行结点Dataset生成");
        return result;
    }


}
