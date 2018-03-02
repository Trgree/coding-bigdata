package org.ace.example;

import org.I0Itec.zkclient.ZkClient;

/**
 * ZkClient 封装好的ZK连接，可以直接使用
 * Created by Liangsj on 2018/3/2.
 */
public class ZkClientTest {
    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("192.168.5.151");
        zkClient.delete("test");
        zkClient.createPersistent("test");
        zkClient.close();
    }
}
