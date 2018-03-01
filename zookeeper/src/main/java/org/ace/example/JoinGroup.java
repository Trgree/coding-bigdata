package org.ace.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

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

    public static void main(String[] args) {

    }
}
