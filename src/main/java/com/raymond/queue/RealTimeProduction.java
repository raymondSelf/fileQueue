package com.raymond.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 异步实时刷盘消费者
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-07-26 17:11
 */
public class RealTimeProduction<E> extends Production<E> {

    private final static Logger logger = LoggerFactory.getLogger(FileQueue.class);

    private Object object = new Object();

    private List<Object> commitQueue = new ArrayList<>();

    private AtomicInteger commitCount = new AtomicInteger();

    private final ReentrantLock commitLock = new ReentrantLock();

    protected RealTimeProduction(String path, String topic) throws IOException {
        super(path, topic);
        forceThread();
    }

    private void forceThread() {
        new Thread(() -> {
            while (true) {
                try {
                    if (commitCount.get() == 0) {
                        wait0();
                    }
                    commitCount.set(0);
                    force();
                } catch (InterruptedException e) {
                    logger.error("刷盘异常：", e);
                }
            }
        }, "实时刷盘线程,topic:" + topic).start();
    }

    private synchronized void wait0() throws InterruptedException {
        wait();
    }

    private synchronized void notifyAll0() {
        commitCount.incrementAndGet();
        notifyAll();
    }

    private void force() {
        ReentrantLock commitLock = this.commitLock;
        commitLock.lock();
        try {
            bufWriteLog.force();
            bufWriteOffsetList.force();
        } finally {
            commitLock.unlock();
        }
    }

    @Override
    public void put(byte[] bytes) {
        super.put(bytes);
        notifyAll0();
    }

    @Override
    protected void logWriteGrow() {
        force();
        ReentrantLock commitLock = this.commitLock;
        commitLock.lock();
        try {
            super.logWriteGrow();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 索引文件扩容
     */
    @Override
    protected void offsetListWriteGrow() {
        force();
        ReentrantLock commitLock = this.commitLock;
        commitLock.lock();
        try {
            super.offsetListWriteGrow();
        } finally {
            commitLock.unlock();
        }
    }
}
