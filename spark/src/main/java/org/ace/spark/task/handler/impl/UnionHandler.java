package org.ace.spark.task.handler.impl;

import com.alibaba.fastjson.JSONObject;
import org.ace.spark.task.DatasetStorage;
import org.ace.spark.task.handler.AbstractHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by Liangsj on 2018/4/2.
 */
public class UnionHandler extends AbstractHandler {

    private static final String UNION = "1";
    private static final String UNION_ALL = "2";

    private String unionTpye = "2"; // 1-union 2-unionAll

    @Override
    protected void initParam() {
        JSONObject obj = JSONObject.parseObject(paramJson);
        unionTpye = obj.getString("unionType");
        logger.info("union结点初始化完成，unionTpye={}", unionTpye);
    }

    @Override
    protected void handleMultiSource() {
        logger.info("对上游的多个输入进行union/unionAll操作");
        Dataset<Row> result = null;
        for (int i = 0; i < parentIds.size(); i++) {
            Dataset<Row> current = DatasetStorage.getDataset(getStorageKey(parentIds.get(i)));
            if(current == null) {
                throw new RuntimeException(sparkNode.getName() + "上游" +parentIds + "输出无输出");
            }
            if(i ==0){
                result = current;// 取得第一个dataset
            } else {// 与其它dataset合并
                if(unionTpye.equals(UNION_ALL)) {
                    logger.info("unionAll dataset");
                    result = result.unionAll(current);
                } else  if(unionTpye.equals(UNION)) {
                    logger.info("union dataset");
                    result = result.unionAll(current).distinct();
                } else {
                    throw new RuntimeException("不支持的union类型：" + unionTpye);
                }

            }
        }
        logger.info("union/unionAll操作完成");
        super.sourceDataset = result;// 赋值合并后结果到sourceDataset

    }

    @Override
    public Dataset<Row> handle(SparkSession sparkSession) {
        return sourceDataset;
    }
}
