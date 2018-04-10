package org.ace.spark.task.handler.impl;


import org.ace.spark.task.utils.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 加载数据实现
 * Text文件格式加载
 * Created by Liangsj on 2018/3/28.
 */
public class TextFileLoadHandler extends FileLoadHandler {

    /**
     * 列数
     * 分隔后列数不等于columnsCount的记录将会补过滤
     */
    private int columnsCount;

    /**
     * 初始化自己的参数
     * 解析paramJson，得到当前结点计算需要的参数
     */
    @Override
    public void initParam() {
        super.initParam();
        srcSep  = StringUtils.escapeExprSpecialWord(srcSep);// 特殊字符，加上\
        columnsCount = jsonObj.getInteger("columnsCount");
        logger.info("输出结点初始化完成，路径={},文件类型={},分隔符={}", input, dataType.getVal(), srcSep);
    }

    /**
     * 加载数据到Dataset
     *
     * @param sparkSession
     * @return
     */
    @Override
    public Dataset<Row> handle(SparkSession sparkSession) {
        logger.info("加载文件,路径={},文件类型=text,分隔符={}",input,srcSep);
        JavaRDD<String[]> source = sparkSession.read().textFile(input).javaRDD()
                .map(l -> l.split(srcSep))
                .filter(arr -> arr.length == columnsCount);

        List<StructField> fields = new ArrayList<>(columnsCount);
        for (int i = 0; i < columnsCount; i++) {
            StructField field = DataTypes.createStructField("_c"+i, DataTypes.StringType, true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);
        JavaRDD<Row> rowRDD = source.map((Function<String[], Row>) arr -> RowFactory.create(arr[0], arr[1].trim()));
        Dataset<Row> result = sparkSession.createDataFrame(rowRDD, schema);
        logger.info("加载文件结点Dataset生成");
        return result;
    }
}
