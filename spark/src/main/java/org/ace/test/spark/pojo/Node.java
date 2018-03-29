package org.ace.test.spark.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 子任务
 * Created by Liangsj on 2018/3/28.
 */
public class Node implements Serializable{
    private int id;
    private String name;
    private String clazz;
    private String paramJson;// json参数
    private List<Integer> parentIds;// 上级的直接结点id

    public Node() {
    }

    public Node(int id, String clazz, String param) {
        this.id = id;
        this.clazz = clazz;
        this.paramJson = param;
    }

    public Node(int id,String name, String clazz, String param,int parentId) {
        this.id = id;
        this.name = name;
        this.clazz = clazz;
        this.paramJson = param;
        addParentId(parentId);

    }

    public void addParentId(int parentId) {
        if(parentIds == null) {
            parentIds = new ArrayList<>();
        }
        this.parentIds.add(parentId);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Integer> getParentIds() {
        return parentIds;
    }

    public void setParentIds(List<Integer> parentIds) {
        this.parentIds = parentIds;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }


    public String getParamJson() {
        return paramJson;
    }

    public void setParamJson(String paramJson) {
        this.paramJson = paramJson;
    }

    @Override
    public String toString() {
        return "Node{" +
                "clazz='" + clazz + '\'' +
                ", paramJson='" + paramJson + '\'' +
                '}';
    }
}
