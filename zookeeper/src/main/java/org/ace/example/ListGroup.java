package org.ace.example;

import org.ace.configupdate.ConnectWatcher;
import org.apache.zookeeper.KeeperException;

import java.util.List;

/**
 * @author L
 * @date 2018/3/2
 */
public class ListGroup extends ConnectWatcher {

    public void list(String groupName) throws Exception {

        String path = "/" + groupName;

        try {
            List<String> children = zk.getChildren(path, false);
            if(children.isEmpty()){
                System.out.printf("No merbers in group %s\n", groupName);
                return;
            }
            children.stream().forEach(System.out :: println);
        } catch (KeeperException.NoNodeException e) {
            System.out.printf("Group %s does not exist\n", groupName);
        }

    }

    public static void main(String[] args) throws Exception {
        ListGroup listGroup = new ListGroup();
        listGroup.connect("120.237.91.36");
        listGroup.list("test");
        listGroup.close();
    }
}
