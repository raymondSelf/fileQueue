package com.raymond.queue.utils;

import com.raymond.queue.impl.FileQueue;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;

/**
 * Mapp工具类
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-01-11 10:21
 */
public class MappedByteBufferUtil {

    public static final long FILE_SIZE = 1024;

    /**
     * 文件名长度
     */
    public static final int NAME_LEN = 19;

    /**
     * 通过offset获取文件名
     * @param topic 时间
     * @param offset offset
     * @return 文件名
     */
    public static String getFileName(String path, String topic, long offset, FileQueue.FileType fileType) {
        path = path + File.separator + topic;
        File file = new File(path);
        File[] files = file.listFiles((dir, name) -> name.endsWith(fileType.name));

        if (files == null || files.length == 0) {
            return String.format("%0" + NAME_LEN + "d", 0) + fileType.name;
        }
        Arrays.sort(files, Comparator.comparingLong(f -> Long.parseLong(f.getName().substring(0, NAME_LEN))));
        String last = files[0].getName();
        for (File file1 : files) {
            String name = file1.getName();
            long offsetName = Long.parseLong(name.substring(0, NAME_LEN));
            if (offset < offsetName) {
                break;
            }
            last = name;
        }
        return last;
    }

    /**
     * 获取消费组在这个文件中是第几条
     * @param readOffset 已读的offset
     * @return 第几条
     * @throws IOException IO异常
     */
    public static long getIndex(String path, String topic,long readOffset) throws IOException {
        RandomAccessFile accessFileLog = null;
        FileChannel fileChannelOffsetList = null;
        try {
            if (readOffset == 0) {
                return 0;
            }
            String logName = MappedByteBufferUtil.getFileName(path, topic, readOffset, FileQueue.FileType.OFFSET_LIST);
            path = path + File.separator + topic;
            accessFileLog = new RandomAccessFile(path + File.separator +
                    logName, "rw");
            fileChannelOffsetList = accessFileLog.getChannel();
            MappedByteBuffer map = fileChannelOffsetList.map(FileChannel.MapMode.READ_ONLY, 0, 8);
            long offsetRead = map.getLong();
            MappedByteBufferUtil.clean(map);
            if (offsetRead == 0 || offsetRead > readOffset) {
                return 0;
            }
            return getReadIndex(readOffset, 0, FILE_SIZE / 8, fileChannelOffsetList) + 1;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        } finally {
            if (fileChannelOffsetList != null) {
                fileChannelOffsetList.close();
            }
            if (accessFileLog != null) {
                accessFileLog.close();
            }

        }
    }

    private static long getReadIndex(long readOffset, long indexMin, long indexMax, FileChannel fileChannelOffsetList) throws IOException {
        long index = (indexMin + indexMax)/2;
        MappedByteBuffer offsetMap = null;
        try {
            offsetMap = fileChannelOffsetList.map(FileChannel.MapMode.READ_ONLY, index * 8, 8);
            long offset = offsetMap.getLong();
            if (offset == readOffset) {
                return index;
            }
            if (offset == 0) {
                return getReadIndex(readOffset, indexMin, index, fileChannelOffsetList);
            }
            if (offset < readOffset) {
                return getReadIndex(readOffset, index, indexMax, fileChannelOffsetList);
            }
            return getReadIndex(readOffset, indexMin, index, fileChannelOffsetList);
        } finally {
            MappedByteBufferUtil.clean(offsetMap);
        }

    }

    /**
     * 清空MappedByteBuffer的数据
     *
     * @param buffer 需要清除的数据
     */
    public static void clean(final MappedByteBuffer buffer) {
        if (null == buffer) {
            return;
        }
        AccessController.doPrivileged((PrivilegedAction<MappedByteBuffer>) () -> {
            try {
                Method getCleanerMethod = buffer.getClass().getMethod(
                        "cleaner");
                getCleanerMethod.setAccessible(true);
                Cleaner cleaner = (Cleaner) getCleanerMethod
                        .invoke(buffer, new Object[0]);
                if (cleaner != null) {
                    cleaner.clean();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }


    /**
     * 存值到MappedByteBufferUtil
     * @param buffer 需要存入的MappedByteBufferUtil
     * @param value 需要存入的值
     */
    public static void putLongToBuffer(MappedByteBuffer buffer, long value) {
        if (!buffer.hasRemaining()) {
            buffer.flip();
        }
        buffer.putLong(value);
    }



    public static void getFileIndex(Map<Long, Long> existFile, FileChannel fileChannel, long offset) throws IOException {
        MappedByteBuffer map = null;
        MappedByteBuffer map1 = null;
        try {
            map = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, 16);
            long fileOffset = map.getLong();
            long index = map.getLong();
            //连续两次为0说明后面都为0
            if (fileOffset == 0 || index == 0) {
//                map1 = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset + 16, 16);
//                if (map1.getLong() == 0 || map1.getLong() == 0) {
                    return;
//                }
            }
            existFile.put(fileOffset, index);
            getFileIndex(existFile, fileChannel, offset + 16);
        } finally {
            MappedByteBufferUtil.clean(map);
            MappedByteBufferUtil.clean(map1);
        }
    }

    /**
     * 获取当前buffer的值(long类型)
     * @param buffer buffer
     * @return 读取的值
     */
    public static long getLongFromBuffer(MappedByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            buffer.flip();
        }
        return buffer.getLong();

    }
    /**
     * 获取当前buffer的值(int类型)
     * @param buffer buffer
     * @return 读取的值
     */
    public static long getIntFromBuffer(MappedByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            buffer.flip();
        }
        return buffer.getInt();
    }

    /**
     * 存值到MappedByteBufferUtil
     * @param buffer 需要存入的MappedByteBufferUtil
     * @param value 需要存入的值
     */
    public static void putIntToBuffer(MappedByteBuffer buffer, int value) {
        if (!buffer.hasRemaining()) {
            buffer.flip();
        }
        buffer.putInt(value);
    }

    /**
     * 是否运行
     * @param isStart 运行判断MappedByteBuffer
     * @param lastRunTime 运行时间MappedByteBuffer
     * @return true:运行,false:停止
     */
    public static boolean isRun(MappedByteBuffer isStart, MappedByteBuffer lastRunTime) {
        long intIsStart = getIntFromBuffer(isStart);
        long longLastRunTime = getLongFromBuffer(lastRunTime);
        long timeMillis = System.currentTimeMillis();
        return intIsStart != 0 && timeMillis - longLastRunTime < 30 * 1000;
    }

    public static boolean isStrEmpty(CharSequence str) {
        return str == null || str.length() == 0;
    }

}
