package org.ace.example;

import org.ace.configupdate.ConnectWatcher;
import org.apache.zookeeper.KeeperException;

import java.util.List;

/**
 * @author L
 * @date 2018/3/2
 */
public class DeleteGroup extends ConnectWatcher {

    public void delete(String groupName) throws KeeperException, InterruptedException {

        if(!groupName.startsWith("/")) {
            groupName = "/" + groupName;
        }
        List<String> children = zk.getChildren(groupName, false);

        try {
           for(String child : children) {
               delete(groupName + "/" + child);
           }
           zk.delete(groupName, -1);
           System.out.printf("Delete  %s \n", groupName);
        } catch (KeeperException.NoNodeException e) {
            System.out.printf("Group %s does not exist\n", groupName);
        }
    }

    public static void main(String[] args) throws Exception {
        DeleteGroup deleteGroup = new DeleteGroup();
        deleteGroup.connect("120.237.91.36");
        deleteGroup.delete("test");
        deleteGroup.close();
    }
}
