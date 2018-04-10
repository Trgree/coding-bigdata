package org.ace.spark.task.pojo;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个任务，包括多个结点（子任务）
 * Created by Liangsj on 2018/3/28.
 */
public class SparkTask implements Serializable {
    private long id;
    private String name;
    private List<SparkNode> sparkNodes;

    public SparkTask() {
    }

    public SparkTask(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public void addNode(SparkNode sparkNode) {
        if (sparkNodes == null) {
            sparkNodes = new ArrayList<>();
        }
        sparkNode.setTaskId(this.id);
        sparkNodes.add(sparkNode);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<SparkNode> getSparkNodes() {
        return sparkNodes;
    }

    public void setSparkNodes(List<SparkNode> sparkNodes) {
        this.sparkNodes = sparkNodes;
    }

    @Override
    public String toString() {
        return "SparkTask{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", sparkNodes=" + sparkNodes +
                '}';
    }

    public static void main(String[] args) {
        SparkTask sparkTask = new SparkTask();
        sparkTask.setId(1);
        sparkTask.addNode(new SparkNode(1, "输入", "org.ace.test.spark.handler.impl.LoadHandler", "{\"path\":\"spark/file/dpi.txt\",\"sep\":\",\"}", -1));
        sparkTask.addNode(new SparkNode(2, "过滤", "org.ace.test.spark.handler.impl.FilterHandler", "{filter:\"ha\"}", 2));
        sparkTask.addNode(new SparkNode(3, "输出", "org.ace.test.spark.handler.impl.SaveHandler", "{\"path\":\"spark/file/out\",\"sep\":\",\"}", 3));
        String json = JSONObject.toJSONString(sparkTask);
        System.out.println(json);
        SparkTask receivedSparkTask = JSONObject.toJavaObject(JSONObject.parseObject(json), SparkTask.class);
        System.out.println(receivedSparkTask);
        receivedSparkTask.getSparkNodes().forEach(System.out::println);

    }
}
