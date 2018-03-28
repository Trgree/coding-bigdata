package org.ace.test.spark;

import com.alibaba.fastjson.JSONObject;
import org.ace.test.spark.pojo.Node;
import org.ace.test.spark.pojo.Task;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Liangsj on 2018/3/28.
 */
public class Context {

    private Task task;

    public Context(Task task) {
        this.task = task;
    }

    public Context(String jsonTask) {
        Task task = JSONObject.toJavaObject(JSONObject.parseObject(jsonTask), Task.class);
        this.task = task;
    }


    public void execute(){
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        ChannelPipeline ch = new ChannelPipeline();
        for(Node node :task.getNodes()){
            ch.addLast(node);
        }

        ch.handle(spark);
        spark.close();
    }


}
