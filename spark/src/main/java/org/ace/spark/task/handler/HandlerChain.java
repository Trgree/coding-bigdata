package org.ace.spark.task.handler;

import org.ace.spark.task.pojo.SparkNode;
import org.ace.spark.task.pojo.SparkTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Handler通道，存放所有Handler，并顺序执行所有Handler
 * Created by Liangsj on 2018/3/16.
 */
public class HandlerChain implements Serializable {
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    private Queue<Handler> handlerSet = new LinkedList<Handler>();
    private SparkTask sparkTask;

    public HandlerChain(SparkTask sparkTask) {
        this.sparkTask = sparkTask;
    }

    public void addLast(Handler handler) {
        handlerSet.add(handler);
    }

    public void addLast(SparkNode sparkNode) throws Exception{
        String clazz = sparkNode.getClazz();
        Object handler = Class.forName(clazz).newInstance();
        if (handler instanceof Handler) {
            // 初始化
            ((Handler) handler).init(sparkTask, sparkNode);
            addLast((Handler) handler);
        } else {
            throw new RuntimeException(clazz + "不是一个Handler实例");
        }
    }


    /**
     * 执行每个Handler
     * @param sparkSession
     */
    public void handle(SparkSession sparkSession) throws Exception {
        for (Handler handler : handlerSet) {
            handler.preHandle();
            Dataset<Row> result = handler.handle(sparkSession);
            handler.postHandle(result);
        }
    }

}
