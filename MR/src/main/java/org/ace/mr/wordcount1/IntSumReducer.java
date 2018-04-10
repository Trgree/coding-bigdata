package org.ace.mr.wordcount1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	private final static Log LOG = LogFactory.getLog(WordCount.class);
	
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
		LOG.info("---reduce输出--- key=" +  key + " value=" + result);
	}
}