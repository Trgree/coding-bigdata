package org.ace.mr.distributedcache;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用DistributedCache分发文件到集群
 *
 * Hadoop提供了两种DistributedCache使用方式，一种是通过API，在程序中设置文件路径，另外一种是通过命令行（-files，-archives或-libjars）参数告诉Hadoop，
 * 个人建议使用第二种方式，该方式可使用以下三个参数设置文件：
 *（1）-files：将指定的本地/hdfs文件分发到各个Task的工作目录下，不对文件进行任何处理；
 *（2）-archives：将指定文件分发到各个Task的工作目录下，并对名称后缀为“.jar”、“.zip”，“.tar.gz”、“.tgz”的文件自动解压，默认情况下，解压后的内容存放到工作目录下名称为解压前文件名的目录中，比如压缩包为dict.zip,则解压后内容存放到目录dict.zip中。为此，你可以给文件起个别名/软链接，比如dict.zip#dict，这样，压缩包会被解压到目录dict中。
 *（3）-libjars：指定待分发的jar包，Hadoop将这些jar包分发到各个节点上后，会将其自动添加到任务的CLASSPATH环境变量中。
 *
 * Created by Liangsj on 2018/4/9.
 */
public class CacheDriver extends Configured implements Tool{


    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new CacheDriver(), args);
        System.exit(exitCode);
    }


    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s needs two arguments    files\n",
                    getClass().getSimpleName());
            return -1;
        }

        Job job = Job.getInstance(getConf(), "CacheDriver");
        job.setJarByClass(CacheDriver.class);
        job.setJobName("Word Counter With Stop Words Removal");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set the MapClass and ReduceClass in the job
        job.setMapperClass(CacheMapper.class);
        job.setReducerClass(CacheReducer.class);

        // 加入到cache,也可以使用命令行（-files，-archives或-libjars）参数告诉Hadoop
        // 对于经常使用的文件或者字典，建议放到HDFS上，这样可以防止每次重复下载
        job.addCacheFile(new Path(args[2]).toUri());

        int returnValue = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}
