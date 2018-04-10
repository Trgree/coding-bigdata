package org.ace.spark.task.handler;

import org.ace.spark.task.DatasetStorage;
import org.ace.spark.task.pojo.SparkNode;
import org.ace.spark.task.pojo.SparkTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Handler 抽象类，实现了接口中部分方法
 * 实现
 * Created by Liangsj on 2018/3/29.
 */
public abstract class AbstractHandler implements Handler {
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected SparkTask sparkTask;
    protected SparkNode sparkNode;

    /**
     * 当前结点
     */
    protected SparkNode currentSparkNode;

    /**
     * 当前结点参数，即每个结点在task-web-job中封装好的参数，格式为json
     */
    protected String paramJson;

    /**
     * 当前结点的上级直接结点
     */
    protected List<Long> parentIds;

    /**
     * 当前结点的输入Dataset，若当前结点有多个，此sourceDataset为合并后数据
     */
    protected Dataset<Row> sourceDataset;

    @Override
    public void init(SparkTask sparkTask, SparkNode currentSparkNode) throws Exception{
        logger.info("结点初始化,当前结点对象:" + currentSparkNode);
        this.sparkTask = sparkTask;
        this.sparkNode = currentSparkNode;
        this.currentSparkNode = currentSparkNode;
        paramJson = currentSparkNode.getParamJson();
        parentIds = currentSparkNode.getParentIds();
        initParam();
    }

    /**
     * 初始化各当前结点的参数
     * 每个结点带有paramJson参数，
     * 该方法一般解析paramJson，得到当前结点计算需要的参数
     */
    protected abstract void initParam() throws Exception;

    /**
     * 数据处理前的操作
     * 得到上结点的输出结点
     * 若上结点多输出，进行相应的联结操作（union, unionAll, join等）
     */
    @Override
    public void preHandle() throws Exception{
        if (parentIds == null || parentIds.isEmpty()) {
            logger.info(currentSparkNode.getName() +  "上结点为空");
            return;
        }
        if (parentIds.size() > 1) {
            handleMultiSource();
        } else {
            sourceDataset = DatasetStorage.getDataset(getStorageKey(parentIds.get(0)));
        }
    }

    protected String getStorageKey(long nodeId){
        return sparkTask.getId() + "_" + nodeId;
    }

    protected String getCurrentStorageKey(){
        return sparkTask.getId() + "_" + sparkNode.getId();
    }

    /**
     * 处理多数据源
     * union join 等多输入结点需实现该方法
     * 根据该结点的父结点id,从缓存中取得所有父结点的Dataset，进行union或join操作后，赋值给sourceDataset属性
     */
    protected void handleMultiSource() {
        // TODO 需要做合并处理 union join
        throw new RuntimeException("暂不支持多输入");
    }

    /**
     * 数据处理结束后，保存Dataset到缓存中
     *
     * @param dataset handle方法返回的结果
     */
    @Override
    public void postHandle(Dataset<Row> dataset) {
        DatasetStorage.addDataset(getCurrentStorageKey(), dataset);
    }
}
