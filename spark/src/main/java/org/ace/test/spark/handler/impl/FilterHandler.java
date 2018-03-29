package org.ace.test.spark.handler.impl;

import com.alibaba.fastjson.JSONObject;
import org.ace.test.spark.DatasetStorage;
import org.ace.test.spark.handler.AbstractHandler;
import org.ace.test.spark.pojo.Node;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 过滤数据实现
 * Created by Liangsj on 2018/3/16.
 */
public class FilterHandler extends AbstractHandler {

    private String filter;

    /**
     * 初始化自己的参数
     */
    @Override
    public void initParam() {
        JSONObject obj = JSONObject.parseObject(paramJson);
        filter =  obj.getString("filter");
        if(filter == null){
            throw new RuntimeException();
        }
    }

    /**
     * 过滤包含filter字符的行
     * @param sparkSession
     * @return
     */
    @Override
    public  Dataset handle(SparkSession sparkSession) {
        if(sourceDataset == null){
            throw new RuntimeException(currentNode.getName() + "无上结点或无输出");
        }
       Dataset result =  sourceDataset.filter((FilterFunction<Row>) row -> {
           for (int i = 0; i < row.length(); i++) {
               String c = row.get(i) + "";
               if(c.contains(filter)){
                   return false;
               }
           }
           return true;
        });
        System.out.println("清洗文件完成");
        return result;
    }


}
