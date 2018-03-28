package org.ace.test.spark;

import org.ace.test.spark.handler.Handler;
import org.ace.test.spark.pojo.Node;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by Liangsj on 2018/3/16.
 */
public class ChannelPipeline {

    private Queue<Handler> handlerSet = new LinkedList<Handler>();

    public void addLast(Handler handler) {
        handlerSet.add(handler);
    }

    public void addLast(Node node){
        String jsonString = node.getParamJson();
        String clazz = node.getClazz();
        try {
            Object handler = Class.forName(clazz).newInstance();
            if(handler instanceof Handler){
                ((Handler) handler).setParam(jsonString);
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
        Dataset current = null;
        Dataset result = null;
        for(Handler handler : handlerSet) {
            result = handler.handle(sparkSession,current);
            current = result;
        }
    }

}
