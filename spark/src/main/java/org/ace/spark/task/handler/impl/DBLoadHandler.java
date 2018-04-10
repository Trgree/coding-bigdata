package org.ace.spark.task.handler.impl;

import com.alibaba.fastjson.JSONObject;
import org.ace.spark.task.handler.AbstractHandler;
import org.ace.spark.task.utils.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * DB输入结点实现
 * Created by Liangsj on 2018/3/16.
 */
public class DBLoadHandler extends AbstractHandler {

    private String url;
    private String password;
    private String user;
    private String table;
    private String driver;
    private String sql;

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
        sql = jsonObj.getString("sql");
        logger.info("写数据库结点初始化完成，url={},user={},table={},driver={}",
                url,user,table,driver);
    }

    /**
     * 数据读取
     *
     * @param sparkSession
     * @return
     */
    @Override
    public Dataset handle(SparkSession sparkSession) throws Exception {
        logger.info("DB表输入,table={},url={}",table, url);
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        connectionProperties.put("driver", driver);

        Dataset<Row> result = sparkSession.read().jdbc(url, table, connectionProperties);
        if(!StringUtils.isBlank(sql)) {
            result.createTempView(table);
            result = sparkSession.sql(sql);
        }
        result.show();
        logger.info("DB输入结点Dataset生成");
        return result;
    }
}
