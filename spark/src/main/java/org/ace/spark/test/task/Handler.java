package org.ace.spark.test.task;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by Liangsj on 2018/3/28.
 */
public interface Handler<T1, T2> extends Serializable{

    T2 handle(SparkSession sparkSession, T1 source);

}
