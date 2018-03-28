package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Liangsj on 2018/2/9.
 */
public class HDFSFileSystemUtil {
    private static final int FALIOVER_SLEEP_MILLIS=500;// 连接失败后重连时间间隔
    private static final int FALIOVER_ATTEMPTS=1;//  连接失败后重连次数

    /**
     * 获取高可用HA HDFS集群的FileSystem
     * @param namenode1 HA其中一台namenode 格式为ip:port 如 192.168.5.150:8020
     * @param namenode2 HA其中第二台namenode 格式为ip:port 如 192.168.5.151:8020
     * @throws IOException
     */
    public static FileSystem getHAFileSystem(String namenode1, String namenode2) throws IOException {
        Configuration  conf = new Configuration(false);
        conf.set("fs.defaultFS", "hdfs://nameservice1");
     //   conf.set("fs.default.name", conf.get("fs.defaultFS"));
        conf.set("dfs.nameservices","nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "namenode1,namenode2");
        conf.set("dfs.namenode.rpc-address.nameservice1.namenode1",namenode1);
        conf.set("dfs.namenode.rpc-address.nameservice1.namenode2",namenode2);
        conf.set("dfs.client.failover.proxy.provider.nameservice1" ,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.setInt("dfs.client.failover.sleep.base.millis",FALIOVER_SLEEP_MILLIS);// 连接失败后重连时间间隔
        conf.setInt("dfs.client.failover.max.attempts",FALIOVER_ATTEMPTS);// 连接失败后重连次数
        FileSystem fs = FileSystem.get(conf);
        if(!checkConn(fs)) {
            throw new RuntimeException("hdfs 连接异常");
        }
        return fs;
    }

    public static boolean checkConn(FileSystem fs){
        if(fs == null){
            return false;
        }
        try {
            return fs.exists(new Path("/"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取HDFS集群的FileSystem,只有一个namenode时使用
     * @param namenodeip hdfs主结点ip
     * @param port hdfs访问端口
     * @throws IOException
     */
    public static FileSystem getFileSystem(String namenodeip, int port) throws IOException {
        Configuration conf = new Configuration(false);
        conf.set("fs.default.name", "hdfs://" + namenodeip + ":" + port);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setInt("dfs.client.failover.sleep.base.millis",FALIOVER_SLEEP_MILLIS);// 连接失败后重连时间间隔
        conf.setInt("dfs.client.failover.max.attempts",FALIOVER_ATTEMPTS);// 连接失败后重连次数
        FileSystem fs = FileSystem.get(conf);
        if(!checkConn(fs)) {
            throw new RuntimeException("hdfs 连接异常");
        }
        return fs;
    }


    public static void main(String[] args) throws Exception {
        FileSystem fs  = HDFSFileSystemUtil.getHAFileSystem("192.168.5.150:8021","192.168.5.151:8021");
      //  FileSystem fs = HDFSFileSystemUtil.getFileSystem("192.168.5.150",8020);
      //  fs.copyToLocalFile(new Path("/tmp/wordcount/input"), new Path("tmp"));
        System.out.println(fs.getScheme());
        System.out.println(fs.exists(new Path("/tmp/wordcount/input")));
    }
}
