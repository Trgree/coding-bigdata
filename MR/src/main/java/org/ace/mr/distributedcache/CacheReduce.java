package org.ace.mr.distributedcache;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce class which is executed after the map class and takes
 * key(word) and corresponding values, sums all the values and write the
 * word along with the corresponding total occurances in the output
 *
 * @author Raman
 */
public class CacheReduce extends Reducer<Text, IntWritable, Text, IntWritable> {


}
