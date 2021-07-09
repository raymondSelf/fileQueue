package com.raymond.queue;


import com.raymond.queue.collection.CollectConsumption;
import com.raymond.queue.collection.CollectProduction;
import com.raymond.queue.utils.DateUtil;
import com.raymond.queue.utils.MappedByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 队列
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2020-12-14 14:47
 */
@SuppressWarnings("all")
public class FileQueue<E> {
    private final static Logger logger = LoggerFactory.getLogger(FileQueue.class);
    /**
     * 文件绝对地址对应的文件队列
     * key:文件绝对地址
     * value:文件队列实例
     */
    private static final Map<String, FileQueue> TOPIC_MAP = new ConcurrentHashMap<>(16);

    public static String DEFAULT_PATH = System.getProperty("user.dir");
    public final boolean isCollect;
    /**
     * 只支持消费
     */
    public static final int IS_CONSUMPTION = 2;
    /**
     * 只支持生产
     */
    public static final int IS_PRODUCTION = 1;
    /**
     * 生产消费都支持,并在初始化时就创建一个生产者和消费者
     */
    public static final int IS_ALL = 3;
    /**
     * 生产者和消费都不创建
     * 需要自己后面创建
     */
    public static final int IS_LAZY = 0;

    private static final ScheduledThreadPoolExecutor heartbeatPoolExecutor = javaScheduledThreadExecutor("heartbeatThread");

    private final ScheduledThreadPoolExecutor cleanPoolExecutor = javaScheduledThreadExecutor("cleanFileThread");

    final Map<String, Consumption<E>> groupMap = new ConcurrentHashMap<>(16);

    private final Class<E> eClass;

    private final String path;

    private final String topic;
    /**
     * 是否是普通队列
     */
    private boolean isOrdinary;

    private Production<E> production;

    private Consumption<E> consumption;

    private boolean openProduction;

    private boolean openConsumption;

    private final int type;

    public static final String DEFAULT_GROUP = "defaultGroup";

    /**
     * 创建普通队列模式
     * @param eClass 队列类型
     * @param topic 主题
     * @param <T> 泛型
     * @return 文件队列
     * @throws Exception 异常
     */
    public static <T> FileQueue<T> ordinary(Class<T> eClass, String topic) throws Exception {
        return instantiation(eClass, DEFAULT_PATH, topic, DEFAULT_GROUP, QueueModel.ORDINARY);
    }

    /**
     * 创建发布订阅模式
     * @param eClass 队列类型
     * @param topic 主题
     * @param <T> 泛型
     * @return 文件队列
     * @throws Exception 异常
     */
    public static <T> FileQueue<T> subscribe(Class<T> eClass, String topic, String groupName) throws Exception {
        return instantiation(eClass, DEFAULT_PATH, topic, groupName, QueueModel.SUBSCRIBE);
    }

    /**
     * 创建文件队列
     * @param eClass 类型
     * @param path 路径
     * @param topic 主题名称
     * @param groupName 消费组,如果是普通队列模式可以不用传
     * @param queueModel 队列模式
     * @param <T> 泛型
     * @return 文件对垒
     * @throws Exception 异常
     */
    public static <T> FileQueue<T> instantiation(Class<T> eClass, String path, String topic, String groupName, QueueModel queueModel) throws Exception {
        return instantiation(eClass, path, topic, groupName, queueModel, GrowMode.CONTINUE_OFFSET, 3);
    }
    /**
     * 创建文件队列
     * @param eClass 类型
     * @param path 路径
     * @param topic 主题名称
     * @param groupName 消费组,如果是普通队列模式可以不用传
     * @param queueModel 队列模式
     * @param growMode 消费组创建模式
     * @param type 1代表只生产, 2代表只消费, 3生产消费都支持
     * @param <T> 泛型
     * @return 文件对垒
     * @throws Exception 异常
     */
    public static <T> FileQueue<T> instantiation(Class<T> eClass, String path, String topic, String groupName,
                                                 QueueModel queueModel, GrowMode growMode, int type) throws Exception {
        File file = new File(path + File.separator + "queue");
        String key = file.getAbsolutePath() + ":" + topic;
        if (TOPIC_MAP.containsKey(key)) {
            return existence(key, eClass, topic, groupName, queueModel, growMode);
        }
        synchronized (FileQueue.class) {
            if (TOPIC_MAP.containsKey(key)) {
                return existence(key, eClass, topic, groupName, queueModel, growMode);
            }
            FileQueue<T> eFileQueue = new FileQueue<>(eClass, file.getAbsolutePath(), topic, groupName, queueModel, type);
            TOPIC_MAP.put(key, eFileQueue);
            if (TOPIC_MAP.size() == 1) {
                runHeartbeat();
                exit();
            }
            return eFileQueue;
        }
    }

    /**
     * 程序退出监听
     */
    private static void exit() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            TOPIC_MAP.forEach((k,v) -> oneExit(v));
        }));
    }

    /**
     * 每个主题退出
     * @param fileQueue 主题的文件队列
     * @param <E>
     */
    private static <E> void oneExit(FileQueue<E> fileQueue) {
        try {
            long timeMillis = System.currentTimeMillis();
            if (fileQueue.openProduction) {
                MappedByteBufferUtil.putIntToBuffer(fileQueue.production.isStart, 0);
            }
            fileQueue.groupMap.forEach((k,v) -> {
                MappedByteBufferUtil.putIntToBuffer(v.isStart, 0);
            });
            logger.info("文件队列正常退出,topic:{}", fileQueue.topic);
        } catch (Exception e) {
            logger.error("文件队列程序退出异常,topic:{}", fileQueue.topic, e);
        }
    }

    /**
     * 运行心跳机制
     * 多进程下只能有一个消费组
     */
    private static void runHeartbeat() {
        heartbeatPoolExecutor.scheduleAtFixedRate(() -> TOPIC_MAP.forEach((k, v) -> heartbeat(v)),
                0, 10000, TimeUnit.MILLISECONDS);
    }

    /**
     * 设置运行心跳
     * @param fileQueue 文件队列
     * @param <E>
     */
    private static <E> void heartbeat(FileQueue<E> fileQueue) {
        try {
            long timeMillis = System.currentTimeMillis();
            if (fileQueue.openProduction) {
                MappedByteBufferUtil.putLongToBuffer(fileQueue.production.lastRunTime, timeMillis);
            }
            fileQueue.groupMap.forEach((k,v) -> {
                MappedByteBufferUtil.putLongToBuffer(v.lastRunTime, timeMillis);
            });
        } catch (Exception e) {
            logger.error("存入心跳时间异常,topic:{}", fileQueue.topic, e);
        }


    }

    private static <T> FileQueue<T> existence(String key, Class<T> eClass, String topic, String groupName,
                                              QueueModel queueModel, GrowMode growMode) throws Exception {
        FileQueue<T> fileQueue = TOPIC_MAP.get(key);
        if (fileQueue.eClass != eClass) {
            throw new RuntimeException("和已有的类型不一致");
        }
        if (fileQueue.groupMap.containsKey(groupName)) {
            return fileQueue;
        }
        if (queueModel == QueueModel.ORDINARY) {
            throw new RuntimeException("普通队列模式不支持不同消费组");
        }
        if (fileQueue.type == IS_PRODUCTION) {
            throw new RuntimeException("没有开启消费者,不支持创建消费组");
        }
        fileQueue.createConsumption(groupName, growMode, "");
        return fileQueue;
    }

    private FileQueue(Class<E> eClass, String path, String topic, String groupName, QueueModel queueModel, int type) throws Exception {
        logger.info("创建文件队列中, topic:{}, groupName:{}, 类型:{}, 开启生成者:{}, 开启消费者:{}",
                topic, groupName, queueModel, (type & 1) == 1, (type & 2) == 2);
        this.eClass = eClass;
        this.path = path;
        this.topic = topic;
        this.type = type;
        this.openProduction = (type & 1) == 1;
        this.openConsumption = (type & 2) == 2;
        this.isOrdinary = queueModel == QueueModel.ORDINARY;
        this.isCollect = MappedByteBufferUtil.isCollection(eClass);
        initFile(topic);
        if (openProduction) {
            production = createProduction(this.path, topic);
            cleanThread();
        }
        if (openConsumption) {
            GrowMode growMode = GrowMode.CONTINUE_OFFSET;
            if (MappedByteBufferUtil.isStrEmpty(groupName) && isOrdinary) {
                groupName = DEFAULT_GROUP;
            }
            Consumption<E> eConsumption = createConsumption(eClass, this.path, topic, groupName, this, growMode, "");
            groupMap.put(groupName, eConsumption);
            if (isOrdinary) {
                consumption = eConsumption;
            }
        }

    }

    private Production<E> createProduction(String path, String topic) throws IOException {
        if (isCollect) {
            return new CollectProduction<>(path, topic);
        }
        return new Production<>(path, topic);
    }

    private Consumption createConsumption(Class<E> eClass, String path, String topic, String groupName,
                                          FileQueue<E> fileQueue, FileQueue.GrowMode growMode, String srcGroupName) throws Exception {
        if (isCollect) {
            return new CollectConsumption(eClass, path, topic, groupName, this, growMode, srcGroupName);
        }
        return new Consumption<>(eClass, path, topic, groupName, this, growMode, srcGroupName);
    }


    private void initFile(String topic) {
        String path = this.path + File.separator + topic;
        File file = new File(path);
        if (!file.exists() && !file.mkdirs()) {
            throw new RuntimeException("文件目录创建失败");
        } else if (!file.isDirectory()) {
            throw new RuntimeException("文件目录创建失败");
        }

    }


    /**
     * 复制消费组
     * 消费组存在的话继续上次消费,消费组不存在的话,复制来源消费组
     * @param groupName 需要创建的消费组
     * @param srcGroupName 来源消费组
     * @return 新的消费组
     * @throws Exception 异常
     */
    public Consumption<E> copyGroup(String groupName, String srcGroupName) throws Exception {
        return copyGroup(groupName, srcGroupName, true);
    }



    /**
     * 复制消费组
     * @param groupName 需要创建的消费组
     * @param srcGroupName 来源消费组
     * @param isContinue
     * true:消费组存在的话继续上次消费,消费组不存在的话,复制来源消费组
     * false:不管消费组是否存在,都复制来源消费组
     * @return 新的消费组
     * @throws Exception 异常
     */
    public Consumption<E> copyGroup(String groupName, String srcGroupName, boolean isContinue) throws Exception {
        return createGroup(groupName, GrowMode.COPY_GROUP, srcGroupName, isContinue);
    }

    public Consumption<E> createFirstGroup(String groupName, boolean isContinue) throws Exception {
        return createGroup(groupName, GrowMode.FIRST_OFFSET, "", isContinue);
    }


    /**
     * 创建消费组
     *
     * @param groupName 消费组名称
     * @param isContinue
     * true:消费组存在的话继续上次消费,消费组不存在的话,从最后一条开始消费
     * false:不管消费组是否存在,都从最后一条开始消费
     * @return 返回消费者
     * @throws Exception 异常
     */
    public Consumption<E> createLastGroup(String groupName, boolean isContinue) throws Exception {
        return createGroup(groupName, GrowMode.LAST_OFFSET, "", isContinue);
    }

    /**
     * 创建消费组
     * @param groupName 消费组名称
     * @param growMode 创建消费组的模式
     * @return 消费组对象
     * @throws Exception 异常
     */
    public Consumption<E> createGroup(String groupName, GrowMode growMode) throws Exception {
        return createGroup(groupName, growMode, "", true);
    }

    /**
     * 创建消费组
     * @param groupName 消费组名称
     * @param growMode 创建消费组的模式
     * @return 消费组对象
     * @throws Exception 异常
     */
    public Consumption<E> createGroup(String groupName, GrowMode growMode, boolean isContinue) throws Exception {
        return createGroup(groupName, growMode, "", isContinue);
    }

    public Consumption<E> createGroup(String groupName, GrowMode growMode, String srcGroupName, boolean isContinue) throws Exception {
        if (isOrdinary) {
            throw new RuntimeException("普通队列模式不支持不同消费组");
        }
        if (groupMap.containsKey(groupName)) {
            return groupMap.get(groupName);
        }
        if (isContinue && existsGroup(groupName)) {
            growMode = GrowMode.CONTINUE_OFFSET;
        }
        return createConsumption(groupName, growMode, srcGroupName);
    }

    private Consumption<E> createConsumption(String groupName, GrowMode growMode, String srcGroupName) throws Exception {
        synchronized (this) {
            if (this.type == FileQueue.IS_LAZY) {
                openConsumption = true;
            }
            if (!openConsumption) {
                throw new RuntimeException("没有开启消费者,不支持创建消费组");
            }
            if (groupMap.containsKey(groupName)) {
                return groupMap.get(groupName);
            }
            Consumption<E> eConsumption = createConsumption(eClass, this.path, topic, groupName, this, growMode, srcGroupName);
            groupMap.put(groupName, eConsumption);
            RandomAccessFile accessReadOffset = new RandomAccessFile(path + File.separator + topic +
                    File.separator + groupName + FileType.READ.name, "rw");
            FileChannel fileChannelReadOffset = accessReadOffset.getChannel();
            MappedByteBuffer hasReadFileSize = fileChannelReadOffset.map(FileChannel.MapMode.READ_WRITE, 16, 8);
            return eConsumption;
        }
    }

    public Production<E> getProduction() {
        return this.production;
    }

    public Production<E> createProduction() throws IOException {
        if (this.type == IS_CONSUMPTION) {
            throw new RuntimeException("没有开启生产者，不支持生产");
        }
        if (this.production == null) {
            this.production = createProduction(this.path, this.topic);
            this.openProduction = true;
        }
        return this.production;
    }


    public Consumption<E> getConsumption() {
        if (!isOrdinary) {
            throw new RuntimeException("请传入消费组的名称");
        }
        return consumption;
    }

    public Consumption<E> getConsumption(String groupName) throws Exception {
        Consumption<E> eConsumption = groupMap.get(groupName);
        if (eConsumption == null) {
            if (existsGroup(groupName)) {
                return createGroup(groupName,GrowMode.CONTINUE_OFFSET);
            } else {
                throw new NullPointerException("当前消费组不存在,请先创建消费组");
            }
        }
        return eConsumption;
    }

    /**
     * 判断消费组是否存在
     * @param groupName 消费组名称
     * @return true 存在
     */
    public boolean existsGroup(String groupName) {
        if (MappedByteBufferUtil.isStrEmpty(groupName)) {
            return false;
        }
        File file = new File(this.path + File.separator + topic + File.separator + groupName + FileType.READ.name);
        return file.exists() && file.isFile();
    }

    public boolean existsTopic() {
        File file = new File(this.path + File.separator + topic + File.separator + "queue" + FileType.WRITE.name);
        return file.exists() && file.isFile();
    }

    public boolean add(E e) {
        put(e);
        return true;
    }

    public E poll(String groupName) throws Exception {
        Consumption<E> eConsumption = groupMap.get(groupName);
        if (eConsumption != null) {
            return eConsumption.poll();
        }
        if (!existsGroup(groupName)) {
            throw new NullPointerException("当前消费组不存在:" + groupName);
        }
        return createGroup(groupName, GrowMode.CONTINUE_OFFSET).poll();
    }


    public E poll() {
        if (!isOrdinary) {
           throw new RuntimeException("请传入消费组的名称");
        }
        return consumption.poll();
    }

    public void put(E e) {
        if (!openProduction) {
            throw new RuntimeException("没有开启生产者，不支持生产");
        }
        production.put(e);
    }

    public int size() {
        if (!isOrdinary) {
            throw new RuntimeException("请传入消费组的名称");
        }
        return (int)(production.getWriteIndex() - consumption.getReadIndex());
    }

    public int size(String groupName) throws Exception {
        Consumption<E> eConsumption = groupMap.get(groupName);
        if (eConsumption != null) {
            return (int)(production.getWriteIndex() - eConsumption.getReadIndex());
        }
        if (!existsGroup(groupName)) {
            throw new RuntimeException("当前消费组不存在:" + groupName);
        }
        return (int)(production.getWriteIndex() - createGroup(groupName, GrowMode.CONTINUE_OFFSET).getReadIndex());
    }


    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean isEmpty(String groupName) throws Exception {
        return size(groupName) == 0;
    }

    public List<E> list(int count) {
        List<E> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            E poll = poll();
            if (poll == null) {
                return list;
            }
            list.add(poll);
        }
        return list;
    }



    private void cleanThread() {
        int period = 24 * 60 * 60 * 1000;
        Date offset = DateUtil.offset(new Date(), 1);
        try {
            Date parse = DateUtil.parse(DateUtil.dateToStr(offset, DateUtil.DateStyle.YYYY_MM_DD) + " 03:00:00");
            long initialDelay = parse.getTime() - System.currentTimeMillis();
            cleanPoolExecutor.scheduleAtFixedRate(this::cleanFile, initialDelay, period, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("定时清理文件线程异常:", e);
        }
    }

    private void cleanFile() {
        try {
            cleanFileByType(FileType.LOG);
            cleanFileByType(FileType.OFFSET_LIST);
        } catch (Exception e) {
            logger.error("清除线程异常:", e);
        }
    }

    private void cleanFileByType(FileType fileType){
        System.gc();
        String path = this.path + File.separator + topic;
        File file = new File(path);
        File[] files = file.listFiles((dir, name) -> name.endsWith(fileType.name));
        int fileNumber = 2;
        if (files == null || files.length < fileNumber) {
            return;
        }
        Arrays.sort(files, Comparator.comparingLong(f -> Long.parseLong(f.getName().substring(0, MappedByteBufferUtil.NAME_LEN))));
        for (int i = 0; i < files.length - fileNumber; i++) {
            delFile(files[i]);
        }
    }

    /**
     * 删除已消费文件
     * @param file 文件
     */
    private void delFile(File file) {
        String name = file.getName();
        name = name.substring(0, name.lastIndexOf("."));
        try {
            if (!isCanDel(name)) {
                logger.info("此文件还未消费,不予许删除,文件名:" + file.getName());
                return;
            }
        } catch (IOException e) {
            logger.error("判断文件是否可以删除异常,文件名:{}", file.getName(), e);
            return;
        }
        if (file.exists()) {
            if (!file.delete()) {
                logger.warn("删除文件失败,文件路径:" + path  + ",文件名:" + file.getName());
            }
            logger.info("删除文件路径:" + path + ",文件名:" + file.getName());
            removeExistFile(name);
        }
    }

    /**
     * 判断文件是否消费完
     * 如果都消费完就可以删除
     * @param name 最小offset
     * @return true可以删除
     * @throws IOException 异常
     */
    private boolean isCanDel(String name) throws IOException {
        List<String> readFile = getReadFile();
        if (readFile == null || readFile.size() < 1) {
            return false;
        }
        for (String fileName : readFile) {
            try (RandomAccessFile accessReadOffset = new RandomAccessFile(path + File.separator + topic +
                    File.separator + fileName, "rw"); FileChannel fileChannelReadOffset = accessReadOffset.getChannel();) {
                MappedByteBuffer hasReadFileSize = fileChannelReadOffset.map(FileChannel.MapMode.READ_WRITE, 16, 8);
                long aLong = hasReadFileSize.getLong();
                MappedByteBufferUtil.clean(hasReadFileSize);
                if (aLong < Long.parseLong(name)) {
                    logger.info("当前主题未消费完,主题名称:{},未消费完的消费组名称:{}", topic, fileName.substring(0, fileName.lastIndexOf(".")));
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 获取所有消费组文件名称
     * @return
     */
    private List<String> getReadFile() {
        File[] files = new File(this.path + File.separator + this.topic).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(FileType.READ.name);
            }
        });
        if (files == null || files.length < 1) {
            return null;
        }
        List<String> names = new ArrayList<>();
        for (File file : files) {
            names.add(file.getName());
        }
        return names;
    }

    private void removeExistFile(String name) {
        ReentrantLock writeLock = production.getWriteLock();
        try {
            writeLock.lock();
            production.getExistFile().remove(Long.parseLong(name));
        } finally {
            writeLock.unlock();
        }

    }

    private static ScheduledThreadPoolExecutor javaScheduledThreadExecutor(String threadName) {
        return new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r);
            thread.setName(threadName);
            thread.setDaemon(true);
            return thread;
        });
    }


    public enum FileType {
        /**
         * 日志文件
         */
        LOG(".log"),
        /**
         * 偏移量文件(索引文件)
         */
        OFFSET_LIST(".offsetList"),
        /**
         * 写文件
         */
        WRITE(".write"),
        /**
         * 读文件
         */
        READ(".read");

        FileType(String name) {
            this.name = name;
        }

        public String name;
    }

    public enum QueueModel {
        /**
         * 普通队列
         * 在多进程下无法会改成发布订阅
         */
        ORDINARY,
        /**
         * 发布订阅
         */
        SUBSCRIBE;
    }

    public enum GrowMode {
        /**
         * 从当前最后一条开始
         */
        LAST_OFFSET,
        /**
         * 从某个消费组开始
         */
        COPY_GROUP,
        /**
         * 从上次消费的开始,如果不存在上次消费,则按最后一条开始
         */
        CONTINUE_OFFSET,
        /**
         * 从首条开始
         */
        FIRST_OFFSET;
    }

}
