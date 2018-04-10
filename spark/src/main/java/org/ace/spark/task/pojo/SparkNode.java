package org.ace.spark.task.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 子任务结点
 * Created by Liangsj on 2018/3/28.
 */
public class SparkNode implements Serializable {
    private long taskId;
    private long id;
    private String name;
    private String clazz;
    private String paramJson;// json参数
    private List<Long> parentIds;// 上级的直接结点id

    public SparkNode() {
    }

    public SparkNode(long id, String clazz, String param) {
        this.id = id;
        this.clazz = clazz;
        this.paramJson = param;
    }

    public SparkNode(long id, String name, String clazz, String param, List<Long> parentIds) {
        this.id = id;
        this.name = name;
        this.clazz = clazz;
        this.paramJson = param;
        this.parentIds = parentIds;
    }

    public SparkNode(long id, String name, String clazz, String param, long parentId) {
        this.id = id;
        this.name = name;
        this.clazz = clazz;
        this.paramJson = param;
        addParentId(parentId);
    }

    public void addParentId(long parentId) {
        if (parentIds == null) {
            parentIds = new ArrayList<>();
        }
        this.parentIds.add(parentId);
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Long> getParentIds() {
        return parentIds;
    }

    public void setParentIds(List<Long> parentIds) {
        this.parentIds = parentIds;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
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
        return "SparkNode{" +
                "taskId=" + taskId +
                ", id=" + id +
                ", name='" + name + '\'' +
                ", clazz='" + clazz + '\'' +
                ", paramJson='" + paramJson + '\'' +
                ", parentIds=" + parentIds +
                '}';
    }
}
