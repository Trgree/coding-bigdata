package org.ace.spark.task;

import com.alibaba.fastjson.JSONObject;
import org.ace.spark.task.pojo.DataType;
import org.ace.spark.task.pojo.SparkNode;
import org.ace.spark.task.pojo.SparkTask;
import org.ace.spark.task.utils.StringUtils;

/**
 * 清洗
 * Created by Liangsj on 2018/3/1.
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        System.setProperty("WORKPATH", "E:\\work\\dev\\IdeaProjects\\task-manage");

        // 模拟出json
        SparkTask sparkTask = new SparkTask();
        sparkTask.setId(1);
        sparkTask.setName("spark demo");

        JSONObject inputParam = new JSONObject();
        inputParam.put("path","task-spark/data/input/dpi2.txt");
        inputParam.put("sep","$$");
        inputParam.put("dataType", DataType.TEXT);
        inputParam.put("columnsCount",2);

        JSONObject dbInputParam = new JSONObject();
        dbInputParam.put("url","jdbc:mysql://192.168.50.155:3306/metadata_v1");
        dbInputParam.put("password","metadata_v1");
        dbInputParam.put("user", "metadata_v1");
        dbInputParam.put("table","test");
        dbInputParam.put("driver","com.mysql.jdbc.Driver");
        dbInputParam.put("sql","select _c0,_c1 from test where _c1>10");


        sparkTask.addNode(new SparkNode(1, StringUtils.encode("输入"), "com.aotain.spark.handler.impl.DBLoadHandler", JSONObject.toJSONString(dbInputParam), -1));
        sparkTask.addNode(new SparkNode(2, StringUtils.encode("过滤"), "com.aotain.spark.handler.impl.FilterHandler", "{filter:\"ha\"}", 1));
        sparkTask.addNode(new SparkNode(3, StringUtils.encode("输出"), "com.aotain.spark.handler.impl.FileSaveHandler", "{\"path\":\"task-spark/data/output\",\"sep\":\",\",\"dataType\":\"CSV\"}", 2));
        String json = JSONObject.toJSONString(sparkTask);
        System.out.println(json);

    //    json = "{\"id\":370,\"name\":\"测试spark\",\"sparkNodes\":[{\"clazz\":\"com.aotain.spark.handler.impl.LoadHandler\",\"id\":3,\"name\":\"读csv\",\"paramJson\":\"{\\\"path\\\":\\\"task-spark/data/input/dpi.txt\\\",\\\"sep\\\":\\\",\\\"}\",\"parentIds\":[1],\"taskId\":370},{\"clazz\":\"com.aotain.spark.handler.impl.FilterHandler\",\"id\":6,\"name\":\"过滤行\",\"paramJson\":\"{\\\"filter\\\":\\\"ha\\\"}\",\"parentIds\":[3],\"taskId\":370},{\"clazz\":\"com.aotain.spark.handler.impl.SaveHandler\",\"id\":5,\"name\":\"写csv\",\"paramJson\":\"{\\\"path\\\":\\\"task-spark/data/output\\\",\\\"sep\\\":\\\"|\\\"}\",\"parentIds\":[6],\"taskId\":370}]}";

        Processor processor = new Processor(json);
        processor.process();

        System.out.println("done");

    }

}
