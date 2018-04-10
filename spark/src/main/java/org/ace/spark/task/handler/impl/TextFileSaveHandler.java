package org.ace.spark.task.handler.impl;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * Text输出结点实现
 * Created by Liangsj on 2018/3/16.
 */
public class TextFileSaveHandler extends FileSaveHandler {



    /**
     * 结果数据输出
     *
     * @param sparkSession
     * @return
     */
    @Override
    public Dataset<Row> handle(SparkSession sparkSession) {
        logger.info("文件输出结点Dataset定义");
        if (sourceDataset == null) {
            throw new RuntimeException(currentSparkNode.getName() + "无上结点或上结点无输出");
        }
        logger.info("保存文件,路径={},文件类型={},分隔符={}",output, dataType.getVal(), desSep);
        Dataset<String> result = sourceDataset.map((MapFunction<Row, String>) row ->
                        row.mkString(desSep)
                , Encoders.STRING());
        result.write()
                .format(dataType.getVal())
                .option("header", "false")
                .save(output);
        logger.info("文件输出完成");
        return null;
    }
}
