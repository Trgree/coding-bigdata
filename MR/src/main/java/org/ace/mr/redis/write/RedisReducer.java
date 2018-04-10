package org.ace.mr.redis.write;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;

/**
 * 数据输出到redis set
 * key为标签，set value为用户名
 *
 * Created by Liangsj on 2018/4/10.
 */
public class RedisReducer extends Reducer<Text, Text, Text, Text> {
    // 订阅通道
    private static final String REDIS_CHANNEL = "Channel_global";
    // 消息码，固定
    private static final String MSG_CODE = "60060";
    // 操作信息
    private static final String MSG_COMMENT = "updateLabelUser";
    private JedisCluster jedisCluster;
    private int keyTimeOut;

    @Override
    protected void setup(Context context)  {
        try {
            JedisClusterTool jedisClusterTool = new JedisClusterTool(context.getConfiguration());
            keyTimeOut = context.getConfiguration().getInt("redis.key.timeout.seconds", -1);
            jedisCluster = jedisClusterTool.getCluster();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("初始化JedisCluster异常");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException {
        if(jedisCluster != null) {
            jedisCluster.close();
        }
    }

    /**
     *
     * @param key sLabelAreaUser:[labelId]:[cityCode]
     * @param values username
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        // 加载前先删除该标签set
        jedisCluster.del(key.toString());
        for(Text  val : values){
            jedisCluster.lpush(key.toString(), val.toString());
            if(keyTimeOut > 0) {
                jedisCluster.expire(key.toString(), keyTimeOut);
            }
           // context.write(key, val);
        }

        publishMsgToRedis(key.toString());
    }

    /**
     * 发布订阅消息到redis通道
     * 消息格式：
     *  消息码:标签id:区域:操作信息
     * @param key sLabelAreaUser:[labelId]:[cityCode]
     */
    private void publishMsgToRedis(String key){
        String arr[] = key.toString().split(":");
        //
        StringBuffer msg = new StringBuffer();
        msg.append(MSG_CODE).append(":")
                .append(arr[1]).append(":")
                .append(arr[2]).append(":")
                .append(MSG_COMMENT);
        jedisCluster.publish(REDIS_CHANNEL, msg.toString());
    }
}
