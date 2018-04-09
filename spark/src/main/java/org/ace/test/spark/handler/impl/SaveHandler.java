package org.ace.test.spark.handler.impl;

import com.alibaba.fastjson.JSONObject;
import org.ace.test.spark.handler.AbstractHandler;
import org.ace.test.spark.pojo.Node;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 输出结点实现
 * Created by Liangsj on 2018/3/16.
 */
public class SaveHandler  extends AbstractHandler {

    private String desSep;

    private String output;

    /**
     * 初始化自己的参数
     * 解析paramJson，得到当前结点计算需要的参数
     */
    @Override
    public void initParam() {
        JSONObject obj = JSONObject.parseObject(paramJson);
        desSep = obj.getString("sep");
        output = obj.getString("path");
    }

    /**
     * 结果数据输出
     * @param sparkSession
     * @return
     */
    @Override
    public Dataset handle(SparkSession sparkSession) {
        if(sourceDataset == null){
            throw new RuntimeException(currentNode.getName() + "无上结点或上结点无输出");
        }
        sourceDataset.write()
                .format("csv")
                .option("header", "false")
                .option("sep",desSep)
                .save(output);
        System.out.println("文件输出完成");
        return null;
    }
}
