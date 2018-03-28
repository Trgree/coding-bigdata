package org.ace.test.spark.pojo;

/**
 * 子任务
 * Created by Liangsj on 2018/3/28.
 */
public class Node {
    private String clazz;
    private String paramJson;// json参数

    public Node() {
    }

    public Node(String clazz, String param) {
        this.clazz = clazz;
        this.paramJson = param;
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
