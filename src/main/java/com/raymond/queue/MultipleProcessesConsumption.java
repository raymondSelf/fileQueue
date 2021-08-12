package com.raymond.queue;

import com.raymond.queue.utils.MappedByteBufferUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 多进程的消费者
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-08-12 15:55
 */
public class MultipleProcessesConsumption<E> extends Consumption<E> {

    private WriteFile writeFile;

    private AtomicLong writeOffset = new AtomicLong();

    MultipleProcessesConsumption(Class<E> eClass, String path, String topic, String groupName, FileQueue<E> fileQueue, FileQueue.GrowMode growMode, String srcGroupName) throws Exception {
        super(eClass, path, topic, groupName, fileQueue);
        this.writeFile = new WriteFile(fileQueue.getProduction(), path, topic);
        super.createFile(groupName, growMode, srcGroupName);
    }

    @Override
    protected boolean isRead() {
        if (readOffset.get() >= writeOffset.get()) {
            setWriteOffset();
            return readOffset.get() < writeOffset.get();
        }
        return true;
    }

    private void setWriteOffset() {
        ReentrantLock writeLock = writeFile.getWriteLock();
        try {
            writeLock.lock();
            long longFromBuffer = writeFile.getNewWriteOffset();
            writeOffset.set(longFromBuffer);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    protected Map<Long, Long> getExistFile() throws IOException {
        return writeFile.getExistFile();
    }

    @Override
    protected AtomicLong getWriteOffset() {
        return writeFile.getWriteOffset();
    }

    @Override
    protected long getWriteIndex() {
        return writeFile.getWriteIndex();
    }

    @Override
    protected ReentrantLock getWriteLock() {
        return writeFile.getWriteLock();
    }

    class WriteFile {

        Production<E> production;

        final RandomAccessFile accessWriteOffset;

        final FileChannel fileChannelWriteOffset;

        final ReentrantLock writeLock;
        /**
         * 最大的offset
         */
        final MappedByteBuffer bufWriteOffset;
        /**
         * 已存最大的下标
         */
        final MappedByteBuffer bufWriteIndex;


        WriteFile(Production<E> production, String path, String topic) throws IOException {
            this.production = production;
            if (this.production == null) {
                writeLock = new ReentrantLock();
            } else {
                writeLock = production.getWriteLock();
            }
            path = path + File.separator + topic;
            accessWriteOffset= new RandomAccessFile(path + File.separator + "queue" + FileQueue.FileType.WRITE.name, "rw");
            fileChannelWriteOffset = accessWriteOffset.getChannel();
            bufWriteOffset = fileChannelWriteOffset.map(FileChannel.MapMode.READ_WRITE, 0, 8);
            bufWriteIndex = fileChannelWriteOffset.map(FileChannel.MapMode.READ_WRITE, 8, 8);
        }

        Map<Long, Long> getExistFile() throws IOException {
            if (this.production != null) {
                return this.production.getExistFile();
            }
            Map<Long, Long> existFile = new HashMap<>(4);
            MappedByteBufferUtil.getFileIndex(existFile, writeFile.fileChannelWriteOffset, Production.EXIST_FILE_OFFSET);
            return existFile;
        }
        AtomicLong getWriteOffset() {
            if (isProduction()) {
                return this.production.getWriteOffset();
            }
            return new AtomicLong(MappedByteBufferUtil.getLongFromBuffer(bufWriteOffset));
        }

        ReentrantLock getWriteLock() {
            if (isProduction()) {
                return this.production.getWriteLock();
            }
            return this.writeLock;
        }

        long getWriteIndex() {
            if (isProduction()) {
                return this.production.getWriteIndex();
            }
            return MappedByteBufferUtil.getLongFromBuffer(bufWriteIndex);
        }

        long getNewWriteOffset() {
            if (isProduction()) {
                return this.production.getWriteOffset().get();
            }
            return MappedByteBufferUtil.getLongFromBuffer(bufWriteOffset);
        }

        private boolean isProduction() {
            return (this.production = fileQueue.getProduction()) != null;
        }
    }
}
