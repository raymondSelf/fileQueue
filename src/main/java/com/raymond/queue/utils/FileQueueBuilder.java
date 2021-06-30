package com.raymond.queue.utils;

import com.raymond.queue.FileQueue;

/**
 * 构建文件队列
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-01-18 15:18
 */
public class FileQueueBuilder<E> {
    /**
     * 泛型类型
     */
    private final Class<E> eClass;
    /**
     * 文件地址
     */
    private String path;
    /**
     * 主题名称
     */
    private final String topic;
    /**
     * 队列模型
     */
    private FileQueue.QueueModel queueModel;
    /**
     * 创建的模型
     */
    private int type;
    /**
     * 消费组创建的类型
     */
    private FileQueue.GrowMode growMode;
    /**
     * 消费组名称
     */
    private String groupName;

    public static <E> FileQueueBuilder<E> create(Class<E> eClass, String topic) {
        return new FileQueueBuilder<>(eClass, topic);
    }

    private FileQueueBuilder(Class<E> eClass, String topic) {
        this.eClass = eClass;
        this.topic = topic;
        this.path = FileQueue.DEFAULT_PATH;
        this.queueModel = FileQueue.QueueModel.ORDINARY;
        this.type = FileQueue.IS_LAZY;
        this.growMode = FileQueue.GrowMode.CONTINUE_OFFSET;
        this.groupName = FileQueue.DEFAULT_GROUP;
    }

    public FileQueueBuilder<E> setPath(String path) {
        this.path = path;
        return this;
    }

    public FileQueueBuilder<E> setQueueModel(FileQueue.QueueModel queueModel) {
        this.queueModel = queueModel;
        return this;
    }

    public FileQueueBuilder<E> setType(int type) {
        this.type = type;
        return this;
    }

    public FileQueueBuilder<E> setGrowMode(FileQueue.GrowMode growMode) {
        this.growMode = growMode;
        return this;
    }

    public FileQueueBuilder<E> setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public FileQueue<E> build() throws Exception {
        if (queueModel == FileQueue.QueueModel.SUBSCRIBE && FileQueue.DEFAULT_GROUP.equals(groupName)) {
            if (type == FileQueue.IS_CONSUMPTION || type == FileQueue.IS_ALL) {
                throw new IllegalArgumentException("参数错误,发布订阅模式请输入消费组");
            }
        }
        return FileQueue.instantiation(eClass, path, topic, groupName, queueModel, growMode, type);
    }


}
