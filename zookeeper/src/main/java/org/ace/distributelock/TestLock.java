package org.ace.distributelock;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁
 *
 * @author L
 * @date 2018/3/3
 */
public class TestLock {
    public static void main(String[] args) {
        Runnable runnable = () -> {
            DistributeLock lock = null;
            try {
                lock = new DistributeLock("120.237.91.36", "test");
                lock.lock();
                System.out.printf("%s 工作\n", Thread.currentThread().getName());
                TimeUnit.SECONDS.sleep(5);
                System.out.printf("%s 结束\n", Thread.currentThread().getName());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if(lock !=null){
                    lock.unlock();
                }
            }
        };

        for(int i=0; i<4; i++) {
            new Thread(runnable).start();
        }

    }
}
