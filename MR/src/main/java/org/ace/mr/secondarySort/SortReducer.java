package org.ace.mr.secondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 同一分区，一同组的数会转入到同一个reduce中
 * 分区和分组都可以自定义，默认是以key为分区和分组，若多个不同的key分为一组，则reduce取输入的第一行中的key为reduce方法的key
 * 
 * @author Liangsj
 *
 */
public class SortReducer extends Reducer<IntPair, IntWritable, IntWritable, IntWritable> {

	private IntWritable keyout = new IntWritable();
	
	@Override
	protected void reduce(IntPair key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		keyout.set(key.getFirst());// key.getSecond(),为第二列的最小值(第一行)
		for(IntWritable val : values) {
			context.write(keyout, val);
		}
		
		// context.write(key,Nullwritable); map端不输出value,这里只输出key,则为取每个first的最小值
	}

	
}
