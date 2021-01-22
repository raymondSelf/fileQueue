package com.raymond.queue;


import com.raymond.queue.utils.MappedByteBufferUtil;
import com.raymond.queue.utils.ProtostuffUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 消费者
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-01-11 10:41
 */
@SuppressWarnings("all")
public class Consumption<E> {
    private final static Logger logger = LoggerFactory.getLogger(Consumption.class);
    /**
     * 读的offset
     */
    private AtomicLong readOffset = new AtomicLong();
    /**
     * 当前写的最大的offset
     */
    private final AtomicLong writeOffset;
    /**
     * 文件队列信息
     */
    private final FileQueue<E> fileQueue;
    /**
     * 生产者信息
     */
    private WriteFile writeFile;
    /**
     * 类型
     */
    private final Class<E> eClass;
    /**
     * 路径
     */
    private final String path;
    /**
     * 主题
     */
    private final String topic;
    /**
     * 读文件
     */
    private final String readKey = "read";
    /**
     * 读日志文件
     */
    private final String readLogKey = "readLog";

    private final Map<String, RandomAccessFile> randomAccessFileMap = new HashMap<>();

    private final Map<String, FileChannel> fileChannelMap = new HashMap<>();

    private final Map<String, MappedByteBuffer> mappedByteBufferMap = new HashMap<>();

    /**
     * 当前已读文件最大的offset
     */
    private MappedByteBuffer readFileSizeMap;
    /**
     * 已读上个log文件最大的offset
     */
    private MappedByteBuffer hasReadFileSizeMap;
    /**
     * 已读上个文件的index
     */
    private MappedByteBuffer hasReadIndexMap;
    /**
     * 读文件
     */
    private MappedByteBuffer bufReadLog;

    /**
     * 已取的offset
     */
    private MappedByteBuffer bufReadOffset;

    /**
     * 读的每条offset集合
     */
    private MappedByteBuffer bufReadOffsetList;

    /**
     * 是否启动
     */
    MappedByteBuffer isStart;
    /**
     * 最后一次运行时间
     */
    MappedByteBuffer lastRunTime;
    /**
     * 读offset的集合
     */
    private final String readOffsetListKey = "readOffsetList";
    /**
     * 读文件日志的容量
     */
    private long readFileSize =  MappedByteBufferUtil.FILE_SIZE;

    private long readOffsetSize = MappedByteBufferUtil.FILE_SIZE / 8;

    private long readIndex = 0;

    private long offsetSize = MappedByteBufferUtil.FILE_SIZE / 8;

    /** 读锁 **/
    private final ReentrantLock readLock = new ReentrantLock();

    Consumption(Class<E> eClass, String path, String topic, String groupName, FileQueue<E> fileQueue,
                FileQueue.GrowMode growMode) throws Exception {
        this(eClass, path, topic, groupName, fileQueue, growMode, "");
    }

    protected Consumption(Class<E> eClass, String path, String topic, String groupName, FileQueue<E> fileQueue,
                          FileQueue.GrowMode growMode, String srcGroupName) throws Exception {
        if (MappedByteBufferUtil.isStrEmpty(groupName)) {
            throw new RuntimeException("消费组的名称不能为空,请输入消费组的名称");
        }
        if (!fileQueue.existsTopic()) {
            throw new RuntimeException("topic:" + topic + ",生产者不存在,无法创建消费者");
        }
        this.eClass = eClass;
        this.path = path;
        this.topic = topic;
        this.fileQueue = fileQueue;
        this.writeFile = new WriteFile(fileQueue.getProduction(), path, topic);
        this.writeOffset = writeFile.getWriteOffset();

        if (growMode == FileQueue.GrowMode.CONTINUE_OFFSET) {
            if (fileQueue.existsGroup(groupName)) {
                initFile(topic, groupName);
            } else {
                logger.warn("当前消费组不存在,使用GrowMode.LAST_OFFSET模式创建消费组");
                growMode = FileQueue.GrowMode.LAST_OFFSET;
            }
        }
        if (growMode == FileQueue.GrowMode.LAST_OFFSET) {
            growGroupLast(topic, groupName);
        }
        if (growMode == FileQueue.GrowMode.COPY_GROUP) {
            if (!fileQueue.existsGroup(srcGroupName)) {
               throw new RuntimeException("来源的消费组不存在,不能够复制消费组,请选择已存在的消费组");
            }
            copyGroup(srcGroupName, groupName);
            initFile(topic, groupName, true);
        }

        if (growMode == FileQueue.GrowMode.FIRST_OFFSET) {
            growGroupFirst(topic, groupName);
        }
    }


    /**
     * 对已有的消费组初始化
     * 非复制的
     * @param topic 主题
     * @param groupName 消费组名称
     * @throws IOException 异常
     */
    private void initFile(String topic, String groupName) throws IOException {
        initFile(topic, groupName, false);
    }

    /**
     * 对已有的消费组初始化
     * @param topic 主题
     * @param groupName 消费组名称
     * @param isCopy 是否是复制的
     * @throws IOException 异常
     */
    private void initFile(String topic, String groupName, boolean isCopy) throws IOException {
        createReadFile(topic, groupName);
        getReadMap();

        if (isRun()) {
            throw new RuntimeException("topic:" + topic + ",消费组已运行,无法重复创建消费组");
        }

        //获取上次已读的offset
        readOffset.set(MappedByteBufferUtil.getLongFromBuffer(bufReadOffset));

        //获取未读的数据
        fileGrow(topic, true, readOffset.get(), readLogKey, FileQueue.FileType.LOG);
        if (MappedByteBufferUtil.getLongFromBuffer(readFileSizeMap) != 0) {
            readFileSize = MappedByteBufferUtil.getLongFromBuffer(readFileSizeMap);
        }
        long hasReadFileSize = MappedByteBufferUtil.getLongFromBuffer(hasReadFileSizeMap);
        bufReadLog = fileChannelMap.get(readLogKey + ":" + topic)
                .map(FileChannel.MapMode.READ_WRITE, readOffset.get() - hasReadFileSize,
                readFileSize - readOffset.get());
        mappedByteBufferMap.put(readLogKey + ":" + topic, bufReadLog);

        //获取未读的offset集合
        readIndex = MappedByteBufferUtil.getIndex(this.path, topic, readOffset.get());
        fileGrow(topic, true, readOffset.get(), readOffsetListKey, FileQueue.FileType.OFFSET_LIST);

        bufReadOffsetList = fileChannelMap.get(readOffsetListKey + ":" + topic).map(FileChannel.MapMode.READ_ONLY, readIndex * 8,
                MappedByteBufferUtil.FILE_SIZE - readIndex * 8);
        long hasReadIndex = MappedByteBufferUtil.getLongFromBuffer(hasReadIndexMap);
        readIndex = hasReadIndex + readIndex;

        readOffsetSize = (readIndex / this.offsetSize + 1) * this.offsetSize;
        mappedByteBufferMap.put(readOffsetListKey + ":" + topic, bufReadOffsetList);
    }

    /**
     * 首条开始创建消费组
     * @param topic 主题
     * @param groupName 消费组
     * @throws IOException 异常
     */
    private void growGroupFirst(String topic, String groupName) throws IOException {
        createReadFile(topic, groupName);
        getReadMap();

        if (isRun()) {
            throw new RuntimeException("topic:" + topic + ",消费组已运行,无法重复创建消费组");
        }

        String logFileName = MappedByteBufferUtil.getFileName(this.path, topic, 0, FileQueue.FileType.LOG);
        long offset = Long.parseLong(logFileName.substring(0, MappedByteBufferUtil.NAME_LEN));

        MappedByteBufferUtil.putLongToBuffer(bufReadOffset, offset);
        readOffset.set(offset);

        //获取未读的数据
        fileGrow(topic, true, readOffset.get(), readLogKey, FileQueue.FileType.LOG);

        readFileSize = offset + MappedByteBufferUtil.FILE_SIZE;
        MappedByteBufferUtil.putLongToBuffer(hasReadFileSizeMap, offset);
        MappedByteBufferUtil.putLongToBuffer(readFileSizeMap, readFileSize);
        bufReadLog = fileChannelMap.get(readLogKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, readOffset.get() - offset,
                readFileSize - readOffset.get());
        mappedByteBufferMap.put(readLogKey + ":" + topic, bufReadLog);

        //获取未读的offset集合
        readIndex = MappedByteBufferUtil.getIndex(this.path, topic, readOffset.get());
        fileGrow(topic, true, readOffset.get(), readOffsetListKey, FileQueue.FileType.OFFSET_LIST);
        bufReadOffsetList = fileChannelMap.get(readOffsetListKey + ":" + topic).map(FileChannel.MapMode.READ_ONLY, readIndex * 8,
                MappedByteBufferUtil.FILE_SIZE - readIndex * 8);

        long hasReadIndex = getHasReadIndex(readOffset.get());
        MappedByteBufferUtil.putLongToBuffer(hasReadIndexMap, hasReadIndex);
        readIndex = hasReadIndex + readIndex;
        readOffsetSize = (readIndex / this.offsetSize + 1) * this.offsetSize;
        mappedByteBufferMap.put(readOffsetListKey + ":" + topic, bufReadOffsetList);
    }

    /**
     * 获取读文件的MappedByteBuffer
     * 0-7代表已读log文件的offset(每读一条更新:bufReadOffset)
     * 8-15代表已读log文件扩容的大小(log扩容后更新:readFileSizeMap)
     * 16-23代表log文件扩容后的上个文件最后的offset(log扩容后更新:hasReadFileSizeMap)
     * 24-31代表扩容后的上个文件的offsetList的最后条数(offsetList扩容后更新:hasReadIndexMap)
     * 32-35代表文件是否启动,(0:未启动,1:启动,isStart)
     * 36-44代表文件的心跳时间,(最后一次更新心跳时间:lastRunTime)
     */
    private void getReadMap() throws IOException {
        bufReadOffset = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 0, 8);
        readFileSizeMap = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 8, 8);
        hasReadFileSizeMap = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 16, 8);
        hasReadIndexMap = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 24, 8);
        isStart = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 32, 4);
        lastRunTime = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 36, 8);
    }

    /**
     * 复制一份消费组文件
     * @param srcGroupName 来源消费组名称
     * @param groupName 新的消费组名称
     * @throws Exception 异常
     */
    private void copyGroup(String srcGroupName, String groupName) throws Exception {
        Consumption<E> consumption = fileQueue.groupMap.get(srcGroupName);
        ReentrantLock readLock;
        if (consumption == null) {
            readLock = new ReentrantLock();
        } else {
            readLock = consumption.readLock;
        }
        try {
            readLock.lock();
            String path = this.path + File.separator + topic + File.separator;
            Files.copy(Paths.get(path + srcGroupName + FileQueue.FileType.READ.name),
                    Paths.get(path + groupName + FileQueue.FileType.READ.name), StandardCopyOption.REPLACE_EXISTING);
        } finally {
            readLock.unlock();
        }

    }

    /**
     * 从最后一条开始
     * @param topic 主题
     * @param groupName 消费组
     * @throws IOException 异常
     */
    private void growGroupLast(String topic, String groupName) throws IOException {
        ReentrantLock writeLock = writeFile.getWriteLock();
        try {
            writeLock.lock();
            createReadFile(topic, groupName);
            getReadMap();
            if (isRun()) {
                throw new RuntimeException("topic:" + topic + ",消费组已运行,无法重复创建消费组");
            }

            bufReadOffset = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 0, 8);
            MappedByteBufferUtil.putLongToBuffer(bufReadOffset, writeOffset.get());
            readOffset.set(writeOffset.get());

            //获取未读的数据
            fileGrow(topic, true, readOffset.get(), readLogKey, FileQueue.FileType.LOG);
            String logFileName = MappedByteBufferUtil.getFileName(this.path, topic, readOffset.get(), FileQueue.FileType.LOG);
            long hasReadFileSize = Long.parseLong(logFileName.substring(0, MappedByteBufferUtil.NAME_LEN));
            readFileSizeMap = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 8, 8);
            hasReadFileSizeMap = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 16, 8);
            MappedByteBufferUtil.putLongToBuffer(hasReadFileSizeMap, hasReadFileSize);
            readFileSize = hasReadFileSize + MappedByteBufferUtil.FILE_SIZE;
            MappedByteBufferUtil.putLongToBuffer(readFileSizeMap, readFileSize);
            bufReadLog = fileChannelMap.get(readLogKey + ":" + topic)
                    .map(FileChannel.MapMode.READ_WRITE, readOffset.get() - hasReadFileSize,
                    readFileSize - readOffset.get());
            mappedByteBufferMap.put(readLogKey + ":" + topic, bufReadLog);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //获取未读的offset集合
            readIndex = MappedByteBufferUtil.getIndex(this.path, topic, readOffset.get());
            long writeIndexIndex = writeFile.getWriteIndex();
            fileGrow(topic, true, readOffset.get(), readOffsetListKey, FileQueue.FileType.OFFSET_LIST);
            bufReadOffsetList = fileChannelMap.get(readOffsetListKey + ":" + topic).map(FileChannel.MapMode.READ_ONLY, readIndex * 8,
                    MappedByteBufferUtil.FILE_SIZE - readIndex * 8);

            long hasReadIndex = getHasReadIndex(readOffset.get());
            MappedByteBufferUtil.putLongToBuffer(hasReadIndexMap, hasReadIndex);
            readIndex = hasReadIndex + readIndex;
            readOffsetSize = (readIndex / this.offsetSize + 1) * this.offsetSize;
            mappedByteBufferMap.put(readOffsetListKey + ":" + topic, bufReadOffsetList);


//            readIndex = writeFile.getWriteIndex();
//            long cuindex = MappedByteBufferUtil.getIndex(this.path, topic, readOffset.get());
//            fileGrow(topic, true, readOffset.get(), readOffsetListKey, FileQueue.FileType.OFFSET_LIST);
//            bufReadOffsetList = fileChannelMap.get(readOffsetListKey + ":" + topic).map(FileChannel.MapMode.READ_ONLY, readIndex % this.offsetSize * 8,
//                    MappedByteBufferUtil.FILE_SIZE - readIndex % this.offsetSize * 8);
//
//            readOffsetSize = (readIndex / this.offsetSize + 1) * this.offsetSize;
//            mappedByteBufferMap.put(readOffsetListKey + ":" + topic, bufReadOffsetList);
//            hasReadIndexMap = fileChannelMap.get(readKey + ":" + topic).map(FileChannel.MapMode.READ_WRITE, 24, 8);
//            MappedByteBufferUtil.putLongToBuffer(hasReadIndexMap, readIndex / this.offsetSize * this.offsetSize);
        } finally {
            writeLock.unlock();
        }
    }


    /**
     * 创建读的文件
     * 0-7代表已读log文件的offset(每读一条更新:bufReadOffset)
     * 8-15代表已读log文件扩容的大小(log扩容后更新:readFileSizeMap)
     * 16-23代表log文件扩容后的上个文件最后的offset(log扩容后更新:hasReadFileSizeMap)
     * 24-31代表扩容后的上个文件的offsetList的最后条数(offsetList扩容后更新:hasReadIndexMap)
     * 32-35代表文件是否启动,0:未启动,1:启动
     * 36-44代表文件的心跳时间,最后一次更新心跳时间
     */
    private void createReadFile(String topic, String groupName) {
        try {
            String path = this.path + File.separator + topic;
            RandomAccessFile accessReadOffset= new RandomAccessFile(path + File.separator + groupName + FileQueue.FileType.READ.name, "rw");
            FileChannel fileChannelReadOffset = accessReadOffset.getChannel();
            randomAccessFileMap.put(readKey + ":" + topic, accessReadOffset);
            fileChannelMap.put(readKey + ":" + topic, fileChannelReadOffset);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建读取日志文件映射地址
     * @param topic 主题名称
     * @param isFirst 程序启动后当天是否第一次执行
     * @param offset 当前offset值
     * @param key 存入的key
     * @param fileType 文件类型
     */
    private void fileGrow(String topic, boolean isFirst, long offset, String key, FileQueue.FileType fileType) {
        try {
            key = key + ":" + topic;
            String logName = MappedByteBufferUtil.getFileName(this.path, topic, offset, fileType);
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

    public E poll() {
        final ReentrantLock lock = this.readLock;
        lock.lock();
        try {
            E e = pollFirst();
            if (e != null) {
                readOffset();
            }
            return e;
        } finally {
            lock.unlock();
        }
    }

    private E pollFirst() {
        if (readOffset.get() >= writeOffset.get()) {
            long longFromBuffer = writeFile.getNewWriteOffset();
            writeOffset.set(longFromBuffer);
            if (readOffset.get() >= writeOffset.get()) {
                return null;
            }
        }
        if (readIndex >= readOffsetSize) {
            offsetListReadGrow();
        }
        long readOffsetListLong = bufReadOffsetList.getLong();
        readIndex++;
        if (readOffsetListLong > readFileSize) {
            logReadGrow(readOffsetListLong);
        }
        int len = (int)(readOffsetListLong - readOffset.get());
        byte[] bytes = new byte[len];
        bufReadLog.get(bytes, 0, len);
        readOffset.addAndGet(len);
//        return JSONObject.parseObject(bytes, eClass);
//        return ProtostuffUtils.deserializer(bytes, eClass);
        return getData(bytes);
    }

    protected E getData(byte[] bytes) {
        return ProtostuffUtils.deserializer(bytes, eClass);
    }

    /**
     * 读日志扩容
     * @param readOffsetListLong 读的offset
     */
    private void logReadGrow(long readOffsetListLong) {
        try {
            fileGrow(topic, false, readOffsetListLong, readLogKey, FileQueue.FileType.LOG);
            bufReadLog = fileChannelMap.get(readLogKey + ":" + topic)
                    .map(FileChannel.MapMode.READ_WRITE, 0, MappedByteBufferUtil.FILE_SIZE);
            readFileSize = MappedByteBufferUtil.FILE_SIZE + readOffset.get();
            MappedByteBufferUtil.putLongToBuffer(hasReadFileSizeMap, readOffset.get());
            MappedByteBufferUtil.putLongToBuffer(readFileSizeMap, readFileSize);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("数据已满，创建新的文件失败");
        }
    }

    private void offsetListReadGrow() {
        try {
            fileGrow(topic, false, readOffset.get(), readOffsetListKey, FileQueue.FileType.OFFSET_LIST);
            bufReadOffsetList = fileChannelMap.get(readOffsetListKey + ":" + topic)
                    .map(FileChannel.MapMode.READ_WRITE, 0, MappedByteBufferUtil.FILE_SIZE);
            readOffsetSize += offsetSize;
            MappedByteBufferUtil.putLongToBuffer(hasReadIndexMap, readIndex);
        } catch (Exception e) {
            throw new RuntimeException("数据已满，创建新的文件失败", e);
        }
    }

    private void readOffset() {
        try {
            bufReadOffset.flip();
            bufReadOffset.putLong(readOffset.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long getReadIndex() {
        return readIndex;
    }

    public long size() {
        return writeFile.getWriteIndex() - readIndex;
    }

    /**
     * 判断是否运行
     * @return true:运行,false:停止(并且更新isStart为运行)
     */
    private boolean isRun() {
        boolean run = MappedByteBufferUtil.isRun(isStart, lastRunTime);
        if (!run) {
            MappedByteBufferUtil.putIntToBuffer(isStart, 1);
        }
        return run;
    }

    private long getHasReadIndex(long offset) throws IOException {
        Map<Long, Long> existFile = writeFile.getExistFile();
        List<Long> list = new ArrayList<>(existFile.keySet());
        list.sort((Comparator.comparingLong(o -> o)));
        long hasReadIndex = 0;
        for (Long fileName : list) {
            if (offset < fileName) {
                break;
            }
            hasReadIndex = existFile.get(fileName);
        }
        return hasReadIndex;
    }

    class WriteFile {

        Production production;

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



        WriteFile(Production production, String path, String topic) throws IOException {
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
            Map<Long, Long> existFile = new HashMap<>();
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
