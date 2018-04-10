package org.ace.spark.task.handler;

import org.ace.spark.task.pojo.SparkNode;
import org.ace.spark.task.pojo.SparkTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * spark数据处理接口
 * 每个结点的处理步骤需实现该接口
 * Created by Liangsj on 2018/3/16.
 */
public interface Handler extends Serializable {

    /**
     * 初始化
     *
     * @param currentSparkNode 当前结点
     */
    void init(SparkTask sparkTask, SparkNode currentSparkNode) throws Exception;

    /**
     * 数据处理前的操作
     */
    void preHandle() throws Exception;

    /**
     * 数据处理
     *
     * @param sparkSession
     * @return 数据处理结果DataSet
     */
    Dataset<Row> handle(SparkSession sparkSession) throws Exception;


    /**
     * 数据处理后操作
     *
     * @param dataset handle方法返回的结果
     */
    void postHandle(Dataset<Row> dataset);
}
