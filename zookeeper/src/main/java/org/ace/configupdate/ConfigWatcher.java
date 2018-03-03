package org.ace.configupdate;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.TimeUnit;

/**
 * 观察Zookeeper中属性的更新情况，并将其打印
 * @author L
 * @date 2018/3/3
 */
public class ConfigWatcher implements Watcher{

    private ActiveKeyValueStore store;
    private static  final String PATH = "/configtest";

    public ConfigWatcher(String hosts) throws Exception {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    public void displayConfig() throws KeeperException, InterruptedException {
        String value = store.read(PATH, this);// 存入当前Water对象，
        System.out.printf("read %s from %s\n", value, PATH);
    }

    @Override
    public void process(WatchedEvent event) {
        // 数据分生改变
        if(event.getType() == Event.EventType.NodeDataChanged) {
            String val = null;
            try {
                val = store.read(PATH, this);
                displayConfig();
            } catch (KeeperException e) {
                System.out.printf("KeeperException: %s. Exiting.\n", e);
            } catch (InterruptedException e) {
                System.err.println("Interrupted. Exiting.");
                Thread.currentThread().interrupt();
            }

        }
    }

    public static void main(String[] args) throws Exception {
        ConfigWatcher configWatcher = new ConfigWatcher("120.237.91.36");
        configWatcher.displayConfig();// 是先读一次
        TimeUnit.HOURS.sleep(Long.MAX_VALUE);
    }
}
