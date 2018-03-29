package org.ace.test.spark.pojo;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个任务，包括多个结点（子任务）
 * Created by Liangsj on 2018/3/28.
 */
public class Task implements Serializable {
    private int id;
    private String name;
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


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        task.addNode(new Node(1,"输入","org.ace.test.spark.handler.impl.LoadHandler","{\"path\":\"spark/file/dpi.txt\",\"sep\":\",\"}", -1));
        task.addNode(new Node(2,"过滤","org.ace.test.spark.handler.impl.FilterHandler","{filter:\"ha\"}",2));
        task.addNode(new Node(3,"输出","org.ace.test.spark.handler.impl.SaveHandler","{\"path\":\"spark/file/out\",\"sep\":\",\"}",3));
        String json = JSONObject.toJSONString(task);
        System.out.println(json);
        Task receivedTask = JSONObject.toJavaObject(JSONObject.parseObject(json), Task.class);
        System.out.println(receivedTask);
        receivedTask.getNodes().forEach(System.out :: println);

    }
}
