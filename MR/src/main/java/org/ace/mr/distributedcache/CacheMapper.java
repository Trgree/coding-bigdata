package org.ace.mr.distributedcache;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * 在Map中使用DistributedCache
 * Created by Liangsj on 2018/4/9.
 */

public class CacheMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Set stopWords = new HashSet();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                for(Path stopWordFile : stopWordsFiles) {
                    readFile(stopWordFile);
                }
            }
        } catch(IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line," ");

        while(st.hasMoreTokens()){
            String wordText = st.nextToken();

            if(!stopWords.contains(wordText.toLowerCase())) {
                word.set(wordText);
                context.write(word,one);
            }
        }

    }

    /**
     * 读取本地文件
     * @param filePath
     */
    private void readFile(Path filePath) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
            String stopWord = null;
            while((stopWord = bufferedReader.readLine()) != null) {
                stopWords.add(stopWord.toLowerCase());
            }
        } catch(IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }
    }
}