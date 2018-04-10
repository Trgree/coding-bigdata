package org.ace.spark.task;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 结点存储
 * Created by Liangsj on 2018/3/29.
 */
public class DatasetStorage {

    /**
     *
     * key为结点taskId_node_id
     * value为结点数据处理结果
     */
    private static Map<String, Dataset<Row>> datasetMap = new ConcurrentHashMap<>();

    public static void addDataset(String key, Dataset<Row> dataset) {
        if(dataset == null){
            return;
        }
        datasetMap.put(key, dataset);
    }

    public static Dataset getDataset(String key) {
        return datasetMap.get(key);
    }
}
