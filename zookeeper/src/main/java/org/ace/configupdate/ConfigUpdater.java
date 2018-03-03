package org.ace.configupdate;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.KeeperException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 不断更新数据到Zookeeper
 *
 * @author L
 * @date 2018/3/3
 */
public class ConfigUpdater {
    private ActiveKeyValueStore store;
    private static final String PATH = "/configtest";
    private Random random = new Random();

    public ConfigUpdater(String hosts) throws Exception {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    public void run() throws KeeperException, InterruptedException {
        while (true) {
            String val = random.nextInt(100) + "";
            store.write(PATH, val);
            System.out.printf("Write %s to %s \n", val, PATH);
            TimeUnit.SECONDS.sleep(3);
        }
    }

    public static void main(String[] args) throws Exception {
        ConfigUpdater updater = new ConfigUpdater("120.237.91.36");
        updater.run();
    }
}
