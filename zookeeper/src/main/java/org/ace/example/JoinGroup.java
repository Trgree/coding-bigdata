package org.ace.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author L
 * @date 2018/3/2
 */
public class JoinGroup extends CreateGroup {

    public void join(String groupName, String memberName) throws Exception {
        String path = "/" + groupName + "/" + memberName;
        //CreateMode.EPHEMERAL: The znode will be deleted upon the client's disconnect
        String createdPath = zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Created " + createdPath);
    }

    public static void main(String[] args) throws Exception {
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect("120.237.91.36");
       // joinGroup.connect("192.168.5.151");
        joinGroup.join("test","child");
        // 程序一直运行，结点就一直存在，程序退出，结点则被删除
        // 利用这一属性，可以监控程序运行状态
        TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
        joinGroup.close();
    }
}
