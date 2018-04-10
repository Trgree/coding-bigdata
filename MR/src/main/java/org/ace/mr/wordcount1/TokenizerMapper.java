package org.ace.mr.wordcount1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static Log LOG = LogFactory.getLog(WordCount.class);
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	enum LineCounter {
			ALL;
	};
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		LOG.info("---map输入--- key=" +  key + " value=" + value);
		System.out.println("---map输入--- key=" +  key + " value=" + value);
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
		context.getCounter(LineCounter.ALL).increment(1);
	}
}
