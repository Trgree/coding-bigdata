package org.ace.test.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * 结点存储
 * Created by Liangsj on 2018/3/29.
 */
public class DatasetStorage {
    private static Map<Integer, Dataset<Row>> nodes = new HashMap<>();

    public static void addDataset(int nodeId, Dataset<Row> dataset){
        nodes.put(nodeId, dataset);
    }

    public static Dataset getDataset(int id){
       return nodes.get(id);
    }
}
