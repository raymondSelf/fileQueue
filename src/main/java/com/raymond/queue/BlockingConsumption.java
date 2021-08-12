package com.raymond.queue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 阻塞的文件队列
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-08-12 14:45
 */
public class BlockingConsumption<E> extends Consumption<E> {

    private BlockingProduction<E> production;


    protected BlockingConsumption(Class<E> eClass, String path, String topic, String groupName, FileQueue<E> fileQueue, FileQueue.GrowMode growMode, String srcGroupName) throws Exception {
        super(eClass, path, topic, groupName, fileQueue);
        if (fileQueue.getProduction() == null) {
            throw new RuntimeException("没有创建生产者,不予许使用阻塞消费者");
        }
        if (!BlockingProduction.class.isAssignableFrom(fileQueue.getProduction().getClass())) {
            throw new RuntimeException("必须是阻塞生产者,不予许使用阻塞消费者");
        }
        this.production = (BlockingProduction<E>) fileQueue.getProduction();
        super.createFile(groupName, growMode, srcGroupName);
    }

    @Override
    protected boolean isRead() {
        return readOffset.get() < production.getWriteOffset().get();
    }


    @Override
    protected Map<Long, Long> getExistFile() {
        return production.getExistFile();
    }

    @Override
    protected AtomicLong getWriteOffset() {
        return production.getWriteOffset();
    }

    @Override
    protected long getWriteIndex() {
        return production.getWriteIndex();
    }

    @Override
    protected ReentrantLock getWriteLock() {
        return production.getWriteLock();
    }



    public E task() throws InterruptedException {
        wait0();
        return super.poll();
    }

    public byte[] taskBytes() throws InterruptedException {
        wait0();
        return super.pollBytes();
    }
    private void wait0() throws InterruptedException {
        while (readOffset.get() >= production.getWriteOffset().get()) {
            production.wait0();
        }
    }



}
