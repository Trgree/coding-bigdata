package org.ace.test.spark.pojo;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个任务，包括多个结点（子任务）
 * Created by Liangsj on 2018/3/28.
 */
public class Task {
    private int id;
    private List<Node> nodes;

    public void addNode(Node node) {
        if(nodes == null){
            nodes = new ArrayList<>();
        }
        nodes.add(node);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    @Override
    public String toString() {
        return "Task{" +
                "id=" + id +
                ", nodes=" + nodes +
                '}';
    }

    public static void main(String[] args) {
        Task task = new Task();
        task.setId(1);
        task.addNode(new Node("org.ace.test.org.ace.spark.LoadHandler","{\"path\":\"spark/file/dpi.txt\",\"sep\":\",\"}"));
        task.addNode(new Node("org.ace.test.org.ace.spark.FilterHandler","{}"));
        task.addNode(new Node("org.ace.test.org.ace.spark.LoadHandler","{\"path\":\"spark/file/out\",\"sep\":\",\"}"));
        String json = JSONObject.toJSONString(task);
        System.out.println(json);
        Task receivedTask = JSONObject.toJavaObject(JSONObject.parseObject(json), Task.class);
        System.out.println(receivedTask);
        receivedTask.getNodes().forEach(System.out :: println);

    }
}
