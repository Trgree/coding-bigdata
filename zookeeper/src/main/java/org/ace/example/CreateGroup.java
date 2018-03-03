package org.ace.example;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 创建组
 *
 * @author L
 * @date 2018/3/2
 */
public class CreateGroup implements Watcher {

    protected ZooKeeper zk;
    // 计算器，计算为1个，即只减小，就countdown完成
    private CountDownLatch connectionSignal = new CountDownLatch(1);
    private static final int SESSION_TIMEOUT = 5000;

    public void connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        // 构造函数立即返回，这里需要等待
        connectionSignal.await();
    }

    @Override
    public void process(WatchedEvent event) {
        if(event.getState() == Event.KeeperState.SyncConnected) {
            // 连接成功，递减计算器，await方法返回
            connectionSignal.countDown();
        }
    }

    public void create(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        // OPEN_ACL_UNSAFE 全部权限
        // PERSISTENT 持久的
        String createPath = zk.create(path, null/*data*/, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("Created " + createPath);
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public static void main(String[] args) throws Exception {
        CreateGroup createGroup = new CreateGroup();
        createGroup.connect("120.237.91.36");
        System.out.println();
        createGroup.create("test");
        createGroup.create("test/c1");
        createGroup.create("test/c1/cc1");
        createGroup.close();
    }
}
