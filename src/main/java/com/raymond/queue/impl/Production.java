package com.raymond.queue.impl;

import com.alibaba.fastjson.JSONObject;
import com.raymond.queue.utils.MappedByteBufferUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 生产者
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-01-11 10:03
 */
@SuppressWarnings("all")
public class Production<E> {

    static final int EXIST_FILE_OFFSET = 44;
    /**
     * 当前写的最大的offset
     */
    private final AtomicLong writeOffset = new AtomicLong();
    /**
     * 写的最大下标
     */
    private final AtomicLong writeIndex = new AtomicLong();

    private final String writeKey = "write";
    /**
     * 实际存储的数据文件
     */
    private final String writeLogKey = "writeLog";
    /**
     * 写offset的集合
     */
    private final String writeOffsetListKey = "writeOffsetList";

    private final String path;

    private final String topic;

    private final Map<String, RandomAccessFile> randomAccessFileMap = new HashMap<>();

    private final Map<String, FileChannel> fileChannelMap = new HashMap<>();

    private final Map<String, MappedByteBuffer> mappedByteBufferMap = new HashMap<>();
    /**
     * 存在的索引文件对应的index
     * 索引文件名,对应第一条的index
     */
    private final Map<Long, Long> existFile = new HashMap<>();

    private long writeFileSize =  MappedByteBufferUtil.FILE_SIZE;

    private final long fileSize =  MappedByteBufferUtil.FILE_SIZE;

    private long offsetSize = MappedByteBufferUtil.FILE_SIZE / 8;
    /**
     * 写offsetListIndex
     */
    private long writeOffsetListIndex = MappedByteBufferUtil.FILE_SIZE / 8;
    /**
     * 写文件
     */
    private MappedByteBuffer bufWriteLog;

    /**
     * log文件倒数第二个文件的最大的offset
     */
    private MappedByteBuffer writtenFileSizeMap;

    /**
     * 写的每条offset集合
     */
    private MappedByteBuffer bufWriteOffsetList;
    /**
     * 最大的offset
     */
    private MappedByteBuffer bufWriteOffset;
    /**
     * log文件最大的offset
     */
    private MappedByteBuffer writeFileSizeMap;
    /**
     * 已存最大的下标
     */
    private MappedByteBuffer bufWriteIndex;


    /**
     * 是否启动
     */
    MappedByteBuffer isStart;
    /**
     * 最后一次运行时间
     */
    MappedByteBuffer lastRunTime;

    /** 写锁 */
    private final ReentrantLock writeLock = new ReentrantLock();

    Production(String path, String topic) throws IOException {
        this.path = path;
        this.topic = topic;
        initFile(topic);
        MappedByteBufferUtil.putIntToBuffer(isStart, 1);
    }

    private void initFile(String topic) throws IOException {
        createWriteFile(topic);
        getWriteMap();
        //获取上次写的offset

        if (MappedByteBufferUtil.isRun(isStart, lastRunTime)) {
            throw new RuntimeException("topic:" + topic + ",生产者已运行,无法重复生产");
        }

        writeOffset.set(MappedByteBufferUtil.getLongFromBuffer(bufWriteOffset));
        //获取未使用的文件
        fileGrow(topic, true, false, writeOffset.get(), writeLogKey, FileQueue.FileType.LOG);
        writeFileSize = MappedByteBufferUtil.getLongFromBuffer(writeFileSizeMap) == 0 ? writeFileSize : MappedByteBufferUtil.getLongFromBuffer(writeFileSizeMap);

        long writtenFileSize = MappedByteBufferUtil.getLongFromBuffer(writtenFileSizeMap);
        bufWriteLog = fileChannelMap.get(writeLogKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, writeOffset.get() - writtenFileSize, writeFileSize - writeOffset.get());
        mappedByteBufferMap.put(writeLogKey + ":" + topic, bufWriteLog);

        //获取上次存的最大的下标
        writeIndex.set(MappedByteBufferUtil.getLongFromBuffer(bufWriteIndex));
        long currentIndex = MappedByteBufferUtil.getIndex(this.path, topic, writeOffset.get());
        if (writeIndex.get() % this.offsetSize != currentIndex) {
            writeIndex.incrementAndGet();
        }
        //获取未使用的文件的offset集合
        writeOffsetListIndex = (writeIndex.get() / this.offsetSize + 1) * this.offsetSize;
        boolean isGrow = writeIndex.get() % offsetSize == 0;
        fileGrow(topic, true, isGrow, writeOffset.get(), writeOffsetListKey, FileQueue.FileType.OFFSET_LIST);
        bufWriteOffsetList = fileChannelMap.get(writeOffsetListKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, writeIndex.get() * 8 % this.fileSize,
                (writeIndex.get() / this.offsetSize + 1) * this.fileSize - writeIndex.get() * 8);
        mappedByteBufferMap.put(writeOffsetListKey + ":" + topic, bufWriteOffsetList);
        MappedByteBufferUtil.getFileIndex(existFile, fileChannelMap.get(writeKey + ":" + topic), EXIST_FILE_OFFSET);
//        if (existFile(0, FileQueue.FileType.OFFSET_LIST)) {
//            existFile.put(0L, 0L);
//        }

    }
    /**
     * 获取读文件的MappedByteBuffer
     * 0-7代表已写的offset(每写一条更新:writeOffset)
     * 8-15代表已写的index条数(每写一条更新:writeIndex)
     * 16-23代表上个文件最后的offset,用于重启后数据恢复找对应文件的对应下标(log扩容后更新:writtenFileSize)
     * 24-31代表最大文件的offset,用于大于后就要扩容(log扩容后更新:writeFileSize)
     * 32-35代表文件是否启动,0:未启动,1:启动
     * 36-44代表文件的心跳时间,最后一次更新心跳时间
     */
    private void getWriteMap() throws IOException {
        bufWriteOffset = fileChannelMap.get(writeKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 0, 8);
        bufWriteIndex = fileChannelMap.get(writeKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 8, 8);
        writtenFileSizeMap = fileChannelMap.get(writeKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 16, 8);
        writeFileSizeMap = fileChannelMap.get(writeKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 24, 8);
        isStart = fileChannelMap.get(writeKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 32, 4);
        lastRunTime = fileChannelMap.get(writeKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 36, 8);
    }


    /**
     * 判断文件是否存在
     * @param offset offset
     * @param fileType 文件类型
     * @return true 存在
     */
    public boolean existFile(long offset, FileQueue.FileType fileType) {
        File file = new File(this.path + File.separator + topic + File.separator + String.format("%0" + MappedByteBufferUtil.NAME_LEN + "d", offset) + fileType.name);
        return file.exists() && file.isFile();
    }

    /**
     * 创建写的文件
     * 0-7代表已写的offset(每写一条更新:writeOffset)
     * 8-15代表已写的index条数(每写一条更新:writeIndex)
     * 16-23代表上个文件最后的offset,用于重启后数据恢复找对应文件的对应下标(log扩容后更新:writtenFileSize)
     * 24-31代表最大文件的offset,用于大于后就要扩容(log扩容后更新:writeFileSize)
     * 32-35代表文件是否启动,0:未启动,1:启动
     * 36-44代表文件的心跳时间,最后一次更新心跳时间
     */
    private void createWriteFile(String topic) {
        try {
            String path = this.path + File.separator + topic;
            RandomAccessFile accessWriteOffset= new RandomAccessFile(path + File.separator + "queue" + FileQueue.FileType.WRITE.name, "rw");
            FileChannel fileChannelWriteOffset = accessWriteOffset.getChannel();
            randomAccessFileMap.put(writeKey + ":" + topic, accessWriteOffset);
            fileChannelMap.put(writeKey + ":" + topic, fileChannelWriteOffset);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建读取日志文件映射地址
     * @param topic 主题名称
     * @param isFirst 程序启动后当天是否第一次执行
     * @param isGrow 是否扩容
     * @param offset 当前offset值
     * @param key 存入的key
     * @param fileType 文件类型
     */
    private void fileGrow(String topic, boolean isFirst, boolean isGrow, long offset, String key, FileQueue.FileType fileType) {
        try {
            key = key + ":" + topic;
            String logName;
            if (isGrow) {
                logName = String.format("%0" + MappedByteBufferUtil.NAME_LEN + "d", offset) + fileType.name;
            } else {
                logName = MappedByteBufferUtil.getFileName(this.path, topic, offset, fileType);
            }
            String path = this.path + File.separator + topic;
            if (!isFirst) {
                MappedByteBufferUtil.clean(mappedByteBufferMap.get(key));
                randomAccessFileMap.get(key).close();
                fileChannelMap.get(key).close();
            }
            RandomAccessFile accessFileLog = new RandomAccessFile(path + File.separator +
                    logName, "rw");
            FileChannel fileChannelLog = accessFileLog.getChannel();
            randomAccessFileMap.put(key, accessFileLog);
            fileChannelMap.put(key, fileChannelLog);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(E e) {
        final ReentrantLock lock = this.writeLock;
        lock.lock();
        try {
            log(JSONObject.toJSONString(e));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将队列数据写文件
     * @param log 需要写的数据
     */
    private void log(String log) {
        byte[] bytes = log.getBytes();
        long offset = writeOffset.get() + bytes.length;
        if (offset > writeFileSize) {
            logWriteGrow();
        }
        bufWriteLog.put(bytes);
        //每条追加一条offset
        if(writeIndex.incrementAndGet() > writeOffsetListIndex) {
            offsetListWriteGrow();
        }
        bufWriteOffsetList.putLong(writeOffset.addAndGet(bytes.length));
        writeOffset();
        writeIndex();
    }


    /**
     * 写日志扩容
     */
    private void logWriteGrow() {
        try {
            fileGrow(topic, false, true, writeOffset.get(), writeLogKey, FileQueue.FileType.LOG);
            bufWriteLog = fileChannelMap.get(writeLogKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            writeFileSize = fileSize + writeOffset.get();
            MappedByteBufferUtil.putLongToBuffer(writtenFileSizeMap, writeOffset.get());
            MappedByteBufferUtil.putLongToBuffer(writeFileSizeMap, writeFileSize);
        } catch (IOException e) {
            throw new RuntimeException("数据已满，创建新的文件失败", e);
        }
    }

    /**
     * 索引文件扩容
     */
    private void offsetListWriteGrow() {
        try {
            fileGrow(topic, false, true, writeOffset.get(), writeOffsetListKey, FileQueue.FileType.OFFSET_LIST);
            bufWriteOffsetList = fileChannelMap.get(writeOffsetListKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            writeOffsetListIndex += offsetSize;
            existFile.put(writeOffset.get(), writeIndex.get() - 1);
            existFilePersistence();
        } catch (Exception e) {
            throw new RuntimeException("数据已满，创建新的文件失败", e);
        }
    }


    private void writeOffset() {
        try {
            bufWriteOffset.flip();
            bufWriteOffset.putLong(writeOffset.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeIndex() {
        try {
            bufWriteIndex.flip();
            bufWriteIndex.putLong(writeIndex.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void existFilePersistence() throws IOException {
        List<Long> list = new ArrayList<>(existFile.keySet());
        list.sort((Comparator.comparingLong(o -> o)));
        int offset = EXIST_FILE_OFFSET;
        for (Long fileName : list) {
            Long index = existFile.get(fileName);
            MappedByteBuffer map = null;
            try {
                map = fileChannelMap.get(writeKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, offset, 16);
                map.putLong(fileName);
                map.putLong(index);
                offset += 16;
            } finally {
                MappedByteBufferUtil.clean(map);
            }
        }
    }

    ReentrantLock getWriteLock() {
        return writeLock;
    }

    public long getWriteIndex() {
        return writeIndex.get();
    }

    AtomicLong getWriteOffset() {
        return writeOffset;
    }



    Map<Long, Long> getExistFile() {
        return existFile;
    }
}
