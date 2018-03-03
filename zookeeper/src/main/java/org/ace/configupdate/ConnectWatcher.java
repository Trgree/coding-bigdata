package org.ace.configupdate;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author L
 * @date 2018/3/2
 */
public class ConnectWatcher implements Watcher{

    protected CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final int SESSION_TIMEOUT = 5000;
    protected ZooKeeper zk;

    public void connect(String hosts) throws Exception {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        countDownLatch.await();
    }

    @Override
    public void process(WatchedEvent event) {
        if(event.getState() == Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}
