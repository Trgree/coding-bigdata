package org.ace.test.spark.handler;

import org.ace.test.spark.pojo.Node;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.List;

/**
 * spark数据处理接口
 * 每个结点的处理步骤需实现该接口
 * Created by Liangsj on 2018/3/16.
 */
public interface Handler extends Serializable{

    /**
     * 初始化
     * @param currentNode
     */
     void init(Node currentNode);

    /**
     * 数据处理前的操作
     */
     void preHandle();

    /**
     * 数据处理
     * @param sparkSession
     * @return
     */
     Dataset<Row> handle(SparkSession sparkSession);

    /**
     * 数据处理后操作
     */
     void postHandle(Dataset<Row> dataset);
}
