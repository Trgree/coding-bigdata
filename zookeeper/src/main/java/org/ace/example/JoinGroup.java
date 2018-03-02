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
        joinGroup.connect("192.168.5.151");
        joinGroup.join("test","child");
        TimeUnit.SECONDS.sleep(20);
        joinGroup.close();
    }
}
