package org.ace.test.spark.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by Liangsj on 2018/3/16.
 */
public interface Handler extends Serializable{
     void setParam(String jsonString);
     Dataset handle(SparkSession sparkSession, Dataset dataset);
}
