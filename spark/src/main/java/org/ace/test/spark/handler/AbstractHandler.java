package org.ace.test.spark.handler;

import org.ace.test.spark.DatasetStorage;
import org.ace.test.spark.pojo.Node;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Handler 抽象类，实现了接口中部分方法
 * 实现
 * Created by Liangsj on 2018/3/29.
 */
public abstract class AbstractHandler implements Handler {

    protected Node currentNode;
    protected String paramJson;
    protected List<Integer> parentIds;
    protected Dataset<Row> sourceDataset;

    @Override
    public void init(Node currentNode) {
        this.currentNode = currentNode;
        paramJson = currentNode.getParamJson();
        parentIds = currentNode.getParentIds();
        initParam();
    }

    /**
     * 初始化各当前结点的参数
     */
    protected abstract void initParam();

    @Override
    public void preHandle() {
        if(parentIds == null || parentIds.isEmpty()){
            System.out.println("上结点为空");
            return;
        }
        if(parentIds.size() > 1) {
            // TODO 需要做合并处理 union join
            throw new RuntimeException("暂不支持多输入");
        } else {
            sourceDataset = DatasetStorage.getDataset(parentIds.get(0));

        }
    }

    /**
     * 数据处理结束后，保存Dataset到缓存中
     * @param dataset
     */
    @Override
    public void postHandle(Dataset<Row> dataset) {
        DatasetStorage.addDataset(currentNode.getId(), dataset);
    }
}
