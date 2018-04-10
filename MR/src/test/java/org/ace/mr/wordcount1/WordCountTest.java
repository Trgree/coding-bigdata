package org.ace.mr.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WordCountTest {
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		TokenizerMapper mapper = new TokenizerMapper();
		IntSumReducer reducer = new IntSumReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		mapDriver.getConfiguration().set("kvsplit", "a");
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(
				"World Bye World"));
		mapDriver.withInput(new LongWritable(), new Text(
				"Hadoop Goodbye Hadoop a"));
		mapDriver.withOutput(new Text("World"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bye"), new IntWritable(1));
		mapDriver.withOutput(new Text("World"), new IntWritable(1));
		mapDriver.withOutput(new Text("Hadoop"), new IntWritable(1));
		mapDriver.withOutput(new Text("Goodbye"), new IntWritable(1));
		mapDriver.withOutput(new Text("Hadoop"), new IntWritable(1));
		mapDriver.withOutput(new Text("a"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(2));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("Hadoop"), values);
		reduceDriver.withOutput(new Text("Hadoop"), new IntWritable(3));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapperReducer() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(
				"World Bye World"));
		mapReduceDriver.withInput(new LongWritable(), new Text(
				"Hadoop Goodbye Hadoop a"));
		mapReduceDriver.withOutput(new Text("Hadoop"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("Bye"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("Hadoop"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("Goodbye"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("a"), new IntWritable(1));
	}

	@Test
	public void testMapperCount() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(
				"World Bye World"));
		mapDriver.withInput(new LongWritable(), new Text(
				"World Bye World"));
		
		mapDriver.withOutput(new Text("World"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bye"), new IntWritable(1));
		mapDriver.withOutput(new Text("World"), new IntWritable(1));
		mapDriver.withOutput(new Text("World"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bye"), new IntWritable(1));
		mapDriver.withOutput(new Text("World"), new IntWritable(1));
		mapDriver.runTest();
		assertEquals("Expected 1 counter increment", 2, mapDriver.getCounters()
				.findCounter(TokenizerMapper.LineCounter.ALL).getValue());
	}
}
