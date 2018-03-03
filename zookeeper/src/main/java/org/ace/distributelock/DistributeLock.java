package org.ace.distributelock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 分布式锁实现
 * 多个客户端请求乐，只有一个能获取锁，其它的按顺序等待，直到上一个锁释放
 *
 *  基于ZooKeeper分布式锁的流程：
 * 1。在zookeeper指定节点（locks）下创建临时顺序节点node_n   n会自增
 * 2。获取locks下所有子节点children
 * 3。对子节点按节点自增序号从小到大排序
 * 4。判断本节点是不是第一个子节点（第一个生成结点），若是，则获取锁；若不是，则监听比该节点小的所有节点的删除事件
 * 5。等到若所有比该节点小的所有节点都不在，获到锁
 * @author L
 * @date 2018/3/3
 */
public class DistributeLock  implements Lock,Watcher {

    private static final String ROOT_LOCK = "/locks";

    // 会话超时时间，单位毫秒，如果会话断开超过这个时间，则认为会话结束，如Kill掉程序超过时间
    private static final int SESSION_TIMEOUT = 5000;

    // 等待锁countDownLatch等待时间，即等待锁如果超过该时间，则认为等到了锁
    private static final int COUNT_DOWN_AWAIT_TIME = 30000;

    private ZooKeeper zk;
    // 计算器
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    // 锁名称
    private String lockName;
    // 当前锁
    private String currentLock;
    // 等待锁，即上一结点
    private String waitLock;

    // 等待锁，即前面的所有结点
    private List<String> waitLocks;

    private String lockPrefix;


    public DistributeLock(String hosts, String lockName)  {
        try {
            /**
             * 初始化连接和根路径
             */
            zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
            countDownLatch.await();
            this.lockName = lockName;
            this.lockPrefix = lockName + "_lock_";
            Stat stat = zk.exists(ROOT_LOCK, false);
            if(stat == null) {
                zk.create(ROOT_LOCK, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            System.out.printf("%s 结点已存在\n", ROOT_LOCK);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // 连接或等待锁释放时countDown
        if(countDownLatch !=null && (event.getState() == Event.KeeperState.SyncConnected || event.getType() == Event.EventType.NodeDeleted)) {
            countDownLatch.countDown();
        }
    }

    @Override
    public void lock() {
        if(this.tryLock()) {
            System.out.printf("%s 获得锁 %s\n", Thread.currentThread().getName(),currentLock );
        } else {
            this.waitForAllPreLock(waitLocks, COUNT_DOWN_AWAIT_TIME, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean tryLock() {

        try {
            // 创建临时顺序结点
            currentLock = zk.create(ROOT_LOCK + "/" + lockPrefix, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + " 创建 currentLock = " + currentLock);
            return isMinNode();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 所有锁结点中，是否是最的那个
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    private boolean isMinNode() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(ROOT_LOCK, false);
        // 取得所有lockname的结点
        List<String> lockNodes = new ArrayList<>();
        children.stream().forEach(n -> {
            if(n.startsWith(lockPrefix)){
                lockNodes.add(n);
            }
        });
        System.out.printf("%s 的所有锁结点：%s\n", lockName, lockNodes);

        // 排序，如果当前创建结点为最小结点（即第一个创建），则认为获取当前锁
        Collections.sort(lockNodes);
        if((currentLock).equals(ROOT_LOCK + "/" + lockNodes.get(0))){
            return true;
        }

        // 如果没有获得锁，则获取上一个结点,作为等待锁
        String currentNode = currentLock.substring(currentLock.lastIndexOf("/") + 1);
        waitLock = lockNodes.get(Collections.binarySearch(lockNodes, currentNode) - 1);
        waitLocks = lockNodes.subList(0, Collections.binarySearch(lockNodes, currentNode));
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if(this.tryLock()) {
            System.out.printf("%s 获得锁 %s\n", Thread.currentThread().getName(),currentLock );
            return true;
        }
        return waitForAllPreLock(waitLocks, time, unit);

    }

    private void waitForLock(String waitLock, long time, TimeUnit unit)  {
        try {
            Stat stat = zk.exists(ROOT_LOCK + "/" + waitLock, true);//
            if(stat != null) {
                System.out.printf("%s 等待锁 %s释放 \n" , Thread.currentThread().getName() ,waitLock);
                countDownLatch = new CountDownLatch(1);
                // 计数等待，若等到前一个节点消失，则precess中进行countDown，停止等待，获取锁
                countDownLatch.await(time, unit);
                countDownLatch = null;
                System.out.printf("%s 锁释放\n" , waitLock);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean waitForAllPreLock(List<String> waitLocks, long time, TimeUnit unit) {
        if(waitLocks.isEmpty()) {
            return true;
        }
        System.out.printf("%s 要等待的锁一共有： %s\n" , Thread.currentThread().getName() ,waitLocks);
        for(int i=waitLocks.size() - 1; i>=0;i--){
            waitForLock(waitLocks.get(i), time, unit);
        }
        System.out.println(Thread.currentThread().getName() + " 等到了锁");
        return true;
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }



    @Override
    public void unlock() {
        try {
            zk.delete(currentLock, -1);
            System.out.printf("%s 释放锁：%s\n", Thread.currentThread().getName(), currentLock);
            currentLock = null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }



}
