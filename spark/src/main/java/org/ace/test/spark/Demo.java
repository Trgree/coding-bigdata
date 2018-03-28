package org.ace.test.spark;

import com.alibaba.fastjson.JSONObject;
import org.ace.test.spark.pojo.Node;
import org.ace.test.spark.pojo.Task;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;

/**
 * 清洗
 * Created by Liangsj on 2018/3/1.
 */
public class Demo {
    public static void main(String[] args) {
        String output = "spark/file/out";

        delLocalDir(output);

        // 模拟出json
        Task task = new Task();
        task.setId(1);
        task.addNode(new Node("org.ace.test.spark.handler.LoadHandler","{\"path\":\"spark/file/dpi.txt\",\"sep\":\",\"}"));
        task.addNode(new Node("org.ace.test.spark.handler.FilterHandler","{}"));
        task.addNode(new Node("org.ace.test.spark.handler.SaveHandler","{\"path\":\"spark/file/out\",\"sep\":\",\"}"));
        String json = JSONObject.toJSONString(task);

        Context context = new Context(json);
        context.execute();


        System.out.println("done");

    }

    private static void delLocalDir(String path){
        if(path !=null && !path.endsWith("hdfs")) {
            try {
                FileUtils.deleteDirectory(new File(path));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
