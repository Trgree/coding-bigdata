package org.ace.test.spark;

import org.ace.test.spark.handler.Handler;
import org.ace.test.spark.pojo.Node;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Handler通道，存放所有Handler，并顺序执行所有Handler
 * Created by Liangsj on 2018/3/16.
 */
public class ChannelPipeline implements Serializable {

    private Queue<Handler> handlerSet = new LinkedList<Handler>();

    public void addLast(Handler handler) {
        handlerSet.add(handler);
    }

    public void addLast(Node node){
        String clazz = node.getClazz();
        try {
            Object handler = Class.forName(clazz).newInstance();
            if(handler instanceof Handler){
                // 初始化
                ((Handler) handler).init(node);
                addLast((Handler)handler);
            } else {
               throw new RuntimeException(clazz + "不是一个Handler实例") ;
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void handle(SparkSession sparkSession){
        for(Handler handler : handlerSet) {
            handler.preHandle();
            Dataset<Row> result = handler.handle(sparkSession);
            handler.postHandle(result);
        }
    }

}
