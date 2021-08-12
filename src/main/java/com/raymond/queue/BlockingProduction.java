package com.raymond.queue;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 阻塞生产者
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-08-12 14:58
 */
public class BlockingProduction<E> extends Production<E> {
    private Condition condition;
    protected BlockingProduction(String path, String topic) throws IOException {
        super(path, topic);
        condition = getWriteLock().newCondition();
    }


    @Override
    public void put(byte[] bytes) {
        super.put(bytes);
        signalAll();
    }

    @Override
    public void put(E bytes) {
        super.put(bytes);
        signalAll();
    }

   void wait0() throws InterruptedException {
        ReentrantLock writeLock = getWriteLock();
        writeLock.lock();
        try {
            condition.await();
        } finally {
            writeLock.unlock();
        }
    }

    private void signalAll() {
        ReentrantLock writeLock = getWriteLock();
        writeLock.lock();
        try {
            condition.signalAll();
        } finally {
            writeLock.unlock();
        }
    }

}
