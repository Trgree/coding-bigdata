package org.ace.mr.wordcount1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 运行：
 * hadoop jar wc.jar com.catt.WordCount <input> <output>
 * 
 * @author Ace
 *
 */
public class WordCount extends Configured implements Tool {

	private final static Log LOG = LogFactory.getLog(WordCount.class);
	
	public static void main(String[] args) throws Exception {
		//System.setProperty("hadoop.home.dir", "E:\\develop\\hadoop\\hadoop-2.3.0\\");
		
		int exitcode = ToolRunner.run(new WordCount(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("主方法运行");
	    //String home = System.getProperty("hadoop.home.dir");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Start.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(3);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
