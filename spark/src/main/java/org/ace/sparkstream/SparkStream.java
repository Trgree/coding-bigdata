package org.ace.sparkstream;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * spark Stream demo
 *
 *
 spark-submit \
 --class org.ace.sparkstream.SparkStream \
 --master local[2] \
 --num-executors 2 \
 --executor-memory 1g \
 --executor-cores 4  \
 sparkstream.jar
 */
public class SparkStream implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws InterruptedException {
		new SparkStream().start();
	}
	
	public void start() throws InterruptedException {
		// spark streaming在启动时会启动两个线程： receving thread和 processing data thread.
		// 创建2个或上的stream线程
		// 要使用yarn运行模式 或多个cpu有服务中使用local[2] 才能测试成功
		SparkConf conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		// 监听socket 在机器上先运行Netcat,
		// nc -l 9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.5.150", 9999);
		//JavaDStream<String> lines = jssc.textFileStream("H:\\temp\\test");
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
		wordCounts.dstream().saveAsTextFiles("/tmp/spark/output/wordcount","spark");// 每次计算完保存在hdfs目录，如： /tmp/spark/output/wordcount-1523693320000.spark
		// wordCounts.dstream().saveAsTextFiles("file:///tmp/spark/output/wordcount","spark");// 保存在本地文件


		jssc.start(); // Start the computation
		jssc.awaitTermination();
	}
}
