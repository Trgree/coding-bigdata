package org.ace.configupdate;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;

/**
 * 数据的读取和写入
 * @author L
 * @date 2018/3/3
 */
public class ActiveKeyValueStore extends ConnectWatcher {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    public void write(String path, String value) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if(stat == null) {
            zk.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zk.setData(path, value.getBytes(CHARSET), -1);
        }
    }

    public String read(String path, Watcher watcher) throws KeeperException, InterruptedException {
        // watcher,当数据更新时，触发观察，watcher的process()方法中对这个事件做出反应
        // Stat用来将信息回传给调用者，调用者可以获取一个znode的数据和无数据，这里对无数据不感兴趣，调为null
        byte[] data = zk.getData(path, watcher, null);
        return new String(data, CHARSET);

    }
}
