package org.ace.spark.task.handler.impl;

import com.alibaba.fastjson.JSONObject;
import org.ace.spark.task.handler.AbstractHandler;
import org.ace.spark.task.pojo.DataType;
import org.ace.spark.task.utils.HDFSUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * 输出结点实现
 * Created by Liangsj on 2018/3/16.
 */
public class FileSaveHandler extends AbstractHandler {

    protected String desSep;
    protected String output;
    protected DataType dataType;// 目前支持csv json parquet格式，text格式使用TextFileSaveHandler

    /**
     * 初始化自己的参数
     * 解析paramJson，得到当前结点计算需要的参数
     */
    @Override
    public void initParam() {
        JSONObject jsonObj = JSONObject.parseObject(paramJson);
        desSep = jsonObj.getString("sep");
        output = jsonObj.getString("path");
        dataType = DataType.valOf(jsonObj.getString("dataType"));
        logger.info("输出结点初始化完成，路径={},文件类型={},分隔符={}", output, dataType.getVal(), desSep);
    }

    @Override
    public void preHandle() throws Exception{
        super.preHandle();
        try {
           if(HDFSUtils.rmDir(output)){
               logger.info("删除目录：" + output);
           }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 结果数据输出
     *
     * @param sparkSession
     * @return
     */
    @Override
    public Dataset handle(SparkSession sparkSession) {
        if (sourceDataset == null) {
            throw new RuntimeException(currentSparkNode.getName() + "无上结点或上结点无输出");
        }
        logger.info("保存文件,路径={},文件类型={},分隔符={}",output, dataType.getVal(), desSep);
        sourceDataset.write()
                .format(dataType.getVal())
                .option("header", "false")
                .option("sep", desSep)
                .save(output);
        logger.info("文件输出完成");
        return null;
    }
}
