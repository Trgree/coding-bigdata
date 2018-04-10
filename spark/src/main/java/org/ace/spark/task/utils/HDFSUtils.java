package org.ace.spark.task.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Liangsj on 2018/3/15.
 */
public class HDFSUtils {

    /**
     * 删除HDFS目录
     * @param path
     * @throws IOException
     */
    public static boolean rmDir(String path) throws IOException {
        Configuration conf = new Configuration();
        Path outputPath = new Path(path);
        FileSystem fileSystem = outputPath.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);// true的意思是，就算output有东西，也一带删除
            return true;
        }
        return false;
    }
}
