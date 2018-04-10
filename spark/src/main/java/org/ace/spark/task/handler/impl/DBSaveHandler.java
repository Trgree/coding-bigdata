package org.ace.spark.task.handler.impl;

import com.alibaba.fastjson.JSONObject;
import org.ace.spark.task.handler.AbstractHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * DB输出结点实现
 * Created by Liangsj on 2018/3/16.
 */
public class DBSaveHandler extends AbstractHandler {

    private String url;
    private String password;
    private String user;
    private String table;
    private String driver;

    private String saveMode;

    /**
     * 初始化自己的参数
     * 解析paramJson，得到当前结点计算需要的参数
     */
    @Override
    public void initParam() {
        JSONObject jsonObj = JSONObject.parseObject(paramJson);
        url = jsonObj.getString("url");
        password = jsonObj.getString("password");
        user = jsonObj.getString("user");
        table = jsonObj.getString("table");
        driver = jsonObj.getString("driver");
        saveMode = jsonObj.getString("saveMode");
        logger.info("写数据库结点初始化完成，url={},user={},table={},driver={},saveMode={}",
                url,user,table,driver,saveMode);
    }

    /**
     * 结果数据输出
     *
     * @param sparkSession
     * @return
     */
    @Override
    public Dataset handle(SparkSession sparkSession) {
        if (sourceDataset == null) {
            throw new RuntimeException(currentSparkNode.getName() + "无上结点或上结点无输出");
        }
        logger.info("输出数据到DB库,table={},url={}",table, url);
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        connectionProperties.put("driver", driver);

        sourceDataset.write()
                .mode(saveMode)
                .jdbc(url, table, connectionProperties);
        logger.info("DB输出完成");
        return null;
    }
}
