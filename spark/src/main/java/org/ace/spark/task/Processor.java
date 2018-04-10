package org.ace.spark.task;

import com.alibaba.fastjson.JSONObject;
import org.ace.spark.task.handler.HandlerChain;
import org.ace.spark.task.pojo.SparkNode;
import org.ace.spark.task.pojo.SparkTask;
import org.ace.spark.task.utils.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 执行器
 * Created by Liangsj on 2018/3/28.
 */
public class Processor implements Serializable {

    private Logger logger;

    private SparkTask sparkTask;

    public Processor(SparkTask sparkTask) {
        this.sparkTask = sparkTask;
    }

    public Processor(String jsonTask) {
        jsonTask = jsonTask.replaceAll("&space;"," ");
        SparkTask sparkTask = JSONObject.toJavaObject(JSONObject.parseObject(jsonTask), SparkTask.class);
        System.setProperty("TASK_ID", sparkTask.getId() + "");// 用户log4j日志文件
        logger = LoggerFactory.getLogger(this.getClass());
        logger.info("任务json参数=" + jsonTask);
        logger.info("任务json解析完成，" + sparkTask);
        this.sparkTask = sparkTask;
    }


    public void process() throws Exception {
        HandlerChain ch = new HandlerChain(sparkTask);
        logger.info("task流程如下:");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < sparkTask.getSparkNodes().size(); i++) {
            SparkNode node = sparkTask.getSparkNodes().get(i);
            // nodeName解码
            String nodeName = StringUtils.decode(node.getName());
            node.setName(nodeName);
            logger.info((i + 1) + "." + nodeName);
        }

        for (SparkNode sparkNode : sparkTask.getSparkNodes()) {
            ch.addLast(sparkNode);
        }
        logger.info("所有结点初始化完成，开始执行任务");

        SparkSession spark = SparkSession
                .builder()
                .appName(sparkTask.getName())
                .master("local") // TODO 升级时去掉
                .getOrCreate();
      //  spark.sparkContext().setLogLevel("ERROR");
        ch.handle(spark);
        logger.info("任务执行完成");
        spark.close();
    }


}
