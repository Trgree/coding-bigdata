package org.ace.mr.redis.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 分地市读取业务标签数据dmp.dm_buslabel_http，写入到redis
 *
 * 输入参数：
 * -D input=data/input
 * -D cityCode=GZ
 * -conf config/redis-config.xml
 *
 * Created by Liangsj on 2018/4/8.
 */
public class RedisDriver extends Configured implements Tool {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Map<String, String> cityMap = new HashMap<>();

    {
        cityMap.put("CZ","445100");
        cityMap.put("DG","441900");
        cityMap.put("FS","440600");
        cityMap.put("GZ","440100");
        cityMap.put("HY","441600");
        cityMap.put("HZ","441300");
        cityMap.put("JM","440700");
        cityMap.put("JY","445200");
        cityMap.put("MM","440900");
        cityMap.put("MZ","441400");
        cityMap.put("QY","441800");
        cityMap.put("ST","440500");
        cityMap.put("SW","441500");
        cityMap.put("SG","440200");
        cityMap.put("SZ","440300");
        cityMap.put("YJ","441700");
        cityMap.put("YF","445300");
        cityMap.put("ZJ","440800");
        cityMap.put("ZQ","441200");
        cityMap.put("ZS","442000");
        cityMap.put("ZH","440400");
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        String input = conf.get("input");
        String city = conf.get("cityCode");

        if (!check(input, "input") || !check(city, "city") || !checkRedisConf(conf)) {
            return 1;
        }

        String cityCode = cityMap.get(city.toUpperCase());
        if(cityCode == null){
            logger.warn("无法映射地市："+ city + ",不做映射处理");
            cityCode = city;
        }
        conf.set("CITY_CODE", cityCode);

        Job job = Job.getInstance(conf);
        job.setJobName("Dmp userLabel to Redis[" + cityCode + "]");
        job.setJarByClass(getClass());
        job.setMapperClass(RedisMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       // job.setOutputFormatClass(RedisHashOutputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setReducerClass(RedisReducer.class);

        String[] inputpaths = getPathStrings(input, conf);  // 以逗号分隔的多个路径
        if(inputpaths == null || inputpaths.length == 0) {
            logger.warn("输入路径没有，程序退出：" + input);
            return 1;
        }

        for(String path: inputpaths) {
            FileInputFormat.addInputPath(job, new Path(path));
            System.out.println("input:");
            System.out.println(path);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private  boolean check(final String value, final String key) {
        if (value == null) {
            logger.error("no " + key + " param, Usage: -D " + key + "=xxx");
            logger.info("Usage: hadoop jar  <jarfile> RedisDriver -D input=xxx -D cityCode=GZ -conf config/redis-config.xml");
            return false;
        }
        return true;
    }

    private  boolean checkRedisConf(Configuration conf){
        if(conf.get("redis.hosts") == null){
            logger.error("没有获取到 redis.hosts 配置，请检查redis配置是否正确");
            return false;
        }
        return true;
    }

    private  String[] getPathStrings(String commaSeparatedPaths, Configuration conf) {
        List<String> pathStrings = new ArrayList<String>();
        String[] arr = commaSeparatedPaths.split(",");
        for(String p : arr) {
            if(hdfsPathExists(p,conf)) {
                pathStrings.add(p);
            }
        }
        return pathStrings.toArray(new String[0]);
    }

    private  boolean hdfsPathExists(String path, Configuration conf){
        if(null==path || path.trim().equals("")) {
            return false;
        }
        try {
            Path pathTarget = new Path(path);
            FileSystem fsTarget = FileSystem.get(URI.create(path),conf);
            return (fsTarget.exists(pathTarget));
        } catch (IOException e) {
            return false;
        }

    }

    public static void main(String[] args)throws Exception {
        int exitCode = ToolRunner.run(new RedisDriver(), args);
        System.exit(exitCode);
    }

}
