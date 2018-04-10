package org.ace.mr.redis.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;


/**
 * Redis OutputFormat
 * 输出数据到Redis set(弃用，改用在Reducer直接输出到redis)
 * Created by Liangsj on 2018/4/8.
 */
public class RedisHashOutputFormat extends OutputFormat<Text, Text> {


    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)  throws IOException {
        try {
            return new RedisHashRecordWriter(job.getConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context){
        return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
    }

    public static class RedisHashRecordWriter extends RecordWriter<Text, Text> {

        private JedisCluster jedisCluster;
        private int keyTimeOut;// 过期秒数

        public RedisHashRecordWriter(Configuration conf) throws Exception {
            JedisClusterTool jedisClusterTool = new JedisClusterTool(conf);
            keyTimeOut = conf.getInt("redis.key.timeout.seconds", -1);
            jedisCluster = jedisClusterTool.getCluster();
        }

        @Override
        public void write(Text key, Text value) {
            jedisCluster.lpush(key.toString(), value.toString());
            if(keyTimeOut > 0) {
                jedisCluster.expire(key.toString(), keyTimeOut);
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            jedisCluster.close();
        }
    }

}
