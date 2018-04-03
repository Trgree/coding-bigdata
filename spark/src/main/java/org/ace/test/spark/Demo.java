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
        task.addNode(new Node(1,"输入","org.ace.test.spark.handler.impl.LoadHandler","{\"path\":\"spark/file/dpi.txt\",\"sep\":\",\"}", -1));
        task.addNode(new Node(2,"过滤","org.ace.test.spark.handler.impl.FilterHandler","{filter:\"ha\"}",1));
        task.addNode(new Node(3,"输出","org.ace.test.spark.handler.impl.SaveHandler","{\"path\":\"spark/file/out\",\"sep\":\",\"}",2));
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
