package org.ace.mr.redis.write;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 读取业务标签数据dmp.dm_buslabel_http
 * Created by Liangsj on 2018/4/8.
 */
public class RedisMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text keyOut = new Text();
    private Text valueOut = new Text();
    private static final String SEP = "\\|";
    private static final String PREFIX = "sLabelAreaUser:";
    private String cityCode;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 获取地市
        cityCode = context.getConfiguration().get("CITY_CODE");
        if(cityCode == null){
            throw new RuntimeException("地市为空");
        }

    }

    /**
     * 输入：
     *  userId|labelId
     * 输入：
     *  keyOut:    sLabelAreaUser:[labelId]:[cityCode]
     *  valueOut:  userId
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        String[] arr = val.split(SEP);
        if(arr.length != 2){
            return;
        }
        keyOut.set(PREFIX + arr[1] + ":"+ cityCode);
        valueOut.set(arr[0]);
        context.write(keyOut, valueOut);
    }
}
