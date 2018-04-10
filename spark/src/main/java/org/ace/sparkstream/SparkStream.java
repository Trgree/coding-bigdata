package org.ace.sparkstream;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStream implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws InterruptedException {
		new SparkStream().start();
	}
	
	public void start() throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		//JavaReceiverInputDStream<String> lines = jssc.socketTextStream("120.237.91.36", 9999);
		JavaDStream<String> lines = jssc.textFileStream("H:\\temp");
		lines.print();

		JavaDStream<String> words = lines.flatMap(
				(FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator());
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				(PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				(Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		//wordCounts.dstream().saveAsTextFiles("spark/file/out","spark");

		jssc.start(); // Start the computation
		jssc.awaitTermination();
	}
}
