package com.raymond;

import com.alibaba.fastjson.JSONObject;
import com.raymond.queue.Consumption;
import com.raymond.queue.FileQueue;
import com.raymond.queue.Production;
import com.raymond.queue.collection.CollectionEntry;
import com.raymond.queue.utils.FileQueueBuilder;
import com.raymond.queue.utils.MappedByteBufferUtil;
import com.raymond.queue.utils.ProtostuffUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 测试类
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-01-15 10:10
 */
public class Main {
    public static void main(String[] args) throws Exception {
//        test5();
//        test4();
//        test2();
//        test3();
//        test1();
//        test6();
//        test();
//        test7();
//        Thread.sleep(Integer.MAX_VALUE);
//        ordinary();
        subscribe();
        Thread.sleep(Integer.MAX_VALUE);
    }

    private static void ordinary() throws Exception {
        ordinaryTest3();
    }

    private static void subscribe() throws Exception {
        subscribeTest1();
    }

    /**
     * 测试普通队列基本功能
     * @throws Exception 异常
     */
    private static void ordinaryTest() throws Exception {
        FileQueue<Test> testFilePlusQueue = FileQueue.ordinary(Test.class, "ordinary");
        for (int i = 0; i < 100; i++) {
            testFilePlusQueue.put(new Test("name" + i));
        }
        Test test;
        while ((test = testFilePlusQueue.poll()) != null ) {
            System.out.println(JSONObject.toJSONString(test));
        }
    }

    /**
     * 测试普通队列性能
     * @throws Exception 异常
     */
    private static void ordinaryTest1() throws Exception {
        FileQueue<Test> testFilePlusQueue = FileQueue.ordinary(Test.class, "ordinary");
        Production<Test> production = testFilePlusQueue.getProduction();
        int count = 10000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            production.put(new Test("name" + i));
        }
        long end = System.currentTimeMillis();
        System.out.println("生产:" + count + "条,数据耗时:" + (end - start) + "毫秒");
        int i = 0;
        Consumption<Test> consumption = testFilePlusQueue.getConsumption();
        start = System.currentTimeMillis();
        while (consumption.poll() != null) {
            i ++;
        }
        end = System.currentTimeMillis();
        System.out.println("消费:" + i + "条,数据耗时:" + (end - start) + "毫秒");
    }


    /**
     * 测试普通队列多线程功能测试
     * @throws Exception 异常
     */
    private static void ordinaryTest2() throws Exception {
        FileQueue<Test> testFilePlusQueue = FileQueue.ordinary(Test.class, "ordinary");
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        for (int i = 0; i < 5; i++) {
            final int a = i;
            new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    testFilePlusQueue.put(new Test(a + "name" + j));
                }
                countDownLatch.countDown();
            }).start();
        }

        for (int i = 0; i < 3; i++) {
            final int a = i;
            new Thread(() -> {
                Test test;
                int count = 0;
                while ((test = testFilePlusQueue.poll()) != null || countDownLatch.getCount() != 0) {
                    if (test == null) {
                        continue;
                    }
                    System.out.println(JSONObject.toJSONString(test));
                    count ++;
                }
                System.out.println("线程:" + a + ",消费:" + count + "条");
            }).start();
        }
    }
    /**
     * 测试普通队列多线程功能性能测试
     * @throws Exception 异常
     */
    private static void ordinaryTest3() throws Exception {
        FileQueue<Test> testFilePlusQueue = FileQueue.ordinary(Test.class, "ordinary");
        final CountDownLatch countDownLatch = new CountDownLatch(5);
        final Production<Test> production = testFilePlusQueue.getProduction();
        for (int i = 0; i < 5; i++) {
            final int a = i;
            new Thread(() -> {
                int count = 10000000;
                long start = System.currentTimeMillis();
                try {
                    for (int j = 0; j < count; j++) {
                        production.put(new Test(a + "name" + j));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                long end = System.currentTimeMillis();
                System.out.println("ordinary生产:" + count + "条,数据耗时:" + (end - start) + "毫秒");
                countDownLatch.countDown();
            }).start();
        }

        for (int i = 0; i < 3; i++) {
            final int a = i;
            new Thread(() -> {
                Test test;
                int count = 0;
                Consumption<Test> consumption = testFilePlusQueue.getConsumption();
                long start = System.currentTimeMillis();
                try {
                    while ((test = consumption.poll()) != null || countDownLatch.getCount() != 0) {
                        if (test == null) {
                            continue;
                        }
                        count ++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                System.out.println("线程:" + a + ",消费:" + count + "条,数据耗时:" + (end - start) + "毫秒");
            }).start();
        }
    }

    /**
     * 测试发布订阅队列基本功能
     * @throws Exception 异常
     */
    private static void subscribeTest() throws Exception {
        FileQueue<Test> subscribe = FileQueue.subscribe(Test.class, "subscribe", "test1");
        Production<Test> production = subscribe.getProduction();
        for (int i = 0; i < 100; i++) {
            production.put(new Test("name" + i));
        }
        Test test;
        Consumption<Test> test1 = subscribe.getConsumption("test1");
        while ((test = test1.poll()) != null ) {
            System.out.println(JSONObject.toJSONString(test));
        }
        Consumption<Test> test2 = subscribe.createGroup("test2", FileQueue.GrowMode.FIRST_OFFSET);
        while ((test = test2.poll()) != null ) {
            System.out.println(JSONObject.toJSONString(test));
        }
    }

    /**
     * 测试发布订阅队列性能
     * @throws Exception 异常
     */
    private static void subscribeTest1() throws Exception {
        FileQueue<Test> subscribe = FileQueue.subscribe(Test.class, "subscribe", "test1");
        Production<Test> production = subscribe.getProduction();

        int count = 10000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            production.put(new Test("name" + i));
//            JSONObject.toJSONString(new Test("name" + i));
        }
        long end = System.currentTimeMillis();
        System.out.println("subscribe,生产:" + count + "条,数据耗时:" + (end - start) + "毫秒");
        Consumption<Test> test1 = subscribe.getConsumption("test1");
        int i = 0;
        start = System.currentTimeMillis();
        while (test1.poll() != null) {
//            if (i > 200000) {
//                break;
//            }
            i ++;
        }
        end = System.currentTimeMillis();
        System.out.println("test1消费:" + i + "条,数据耗时:" + (end - start) + "毫秒");
        i = 0;
        Consumption<Test> test2 = subscribe.createGroup("test2", FileQueue.GrowMode.FIRST_OFFSET);
        start = System.currentTimeMillis();
        while (test2.poll() != null) {
            i ++;
        }
        end = System.currentTimeMillis();
        System.out.println("test2消费:" + i + "条,数据耗时:" + (end - start) + "毫秒");
    }

    /**
     * 测试发布订阅队列多线程测试
     * @throws Exception 异常
     */
    private static void subscribeTest2() throws Exception {
        FileQueue<Test> subscribe = FileQueue.subscribe(Test.class, "subscribe", "test1");
        Production<Test> production = subscribe.getProduction();
        CountDownLatch countDownLatch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            final int a = i;
            new Thread(() -> {
                int count = 10000000;
                long start = System.currentTimeMillis();
                try {
                    for (int j = 0; j < count; j++) {
                        production.put(new Test(a + "name" + j));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                System.out.println("subscribe生产:" + count + "条,数据耗时:" + (end - start) + "毫秒");
                countDownLatch.countDown();
            }).start();
        }

        Consumption<Test> test1 = subscribe.getConsumption("test1");
        for (int i = 0; i < 3; i++) {
            final int a = i;
            new Thread(() -> {
                Test test;
                int count = 0;
                long start = System.currentTimeMillis();
                try {
                    while ((test = test1.poll()) != null || countDownLatch.getCount() != 0) {
                        if (test == null) {
                            continue;
                        }
                        count ++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                System.out.println("test1线程:" + a + ",消费:" + count + "条,数据耗时:" + (end - start) + "毫秒");
            }).start();
        }
        Consumption<Test> test2 = subscribe.createGroup("test2", FileQueue.GrowMode.FIRST_OFFSET);
        for (int i = 0; i < 3; i++) {
            final int a = i;
            new Thread(() -> {
                Test test;
                int count = 0;
                long start = System.currentTimeMillis();
                try {
                    while ((test = test2.poll()) != null || countDownLatch.getCount() != 0) {
                        if (test == null) {
                            continue;
                        }
                        count ++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long end = System.currentTimeMillis();
                System.out.println("test2线程:" + a + ",消费:" + count + "条,数据耗时:" + (end - start) + "毫秒");
            }).start();
        }
    }


    /**
     * 测试发布订阅队列性能
     * @throws Exception 异常
     */
    private static void subscribeTest3() throws Exception {
//        FileQueue<Test> subscribe = FileQueue.subscribe(Test.class, "subscribe", "test1");
        FileQueue<Test> subscribe = FileQueueBuilder.create(Test.class, "subscribe").setType(1)
                .setQueueModel(FileQueue.QueueModel.SUBSCRIBE).build();
        Production<Test> production = subscribe.createProduction();
        int count = 10000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            production.put(new Test("name" + i));
        }
        long end = System.currentTimeMillis();
        System.out.println("subscribe,生产:" + count + "条,数据耗时:" + (end - start) + "毫秒");

    }


    private static void test3() throws Exception {

        FileQueue<Test> subscribe = FileQueue.subscribe(Test.class, "test3", "test3");
        Production<Test> production = subscribe.getProduction();
        Consumption<Test> test3 = subscribe.getConsumption("test3");
//        Consumption<Test> test4 = subscribe.createGroup("test4", FileQueueImpl.GrowMode.CONTINUE_OFFSET);
//        Consumption<Test> test5 = subscribe.copyGroup("test5", "test3", false);
//        Consumption<Test> test6 = subscribe.createFirstGroup("test6", true);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            production.put(new Test("name" + i));
        }
        System.out.println(System.currentTimeMillis() - start);
        Test test;
        int i = 0;
        start = System.currentTimeMillis();
//        while ((test = test3.poll()) != null) {
////            System.out.println("test3消费:" + JSONObject.toJSONString(test));
//            i ++;
//        }
//        System.out.println(System.currentTimeMillis() - start);
//        while ((test = test4.poll()) != null) {
//            System.out.println("test4消费:" + JSONObject.toJSONString(test));
//            i ++;
//        }
//        System.out.println(System.currentTimeMillis() - start);
//        while ((test = test5.poll()) != null) {
//            System.out.println("test5消费:" + JSONObject.toJSONString(test));
//            i ++;
//        }
//        System.out.println(System.currentTimeMillis() - start);
//        while ((test = subscribe.poll("test6")) != null) {
//            System.out.println("test6消费:" + JSONObject.toJSONString(test));
//            i ++;
//        }
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(i);
    }

    private static void test2() throws Exception {

//        Production<Test> production = FileQueueImpl.instantiation(Test.class, System.getProperty("user.dir"), "test").getProduction();
//        Consumption<Test> consumption = FileQueueImpl.ordinary(Test.class, "test").getConsumption();
//        FileQueue<Map> fileQueue = FileQueue.subscribe(Map.class, "test", "test");
        FileQueue<Map> fileQueue = FileQueueBuilder.create(Map.class, "test1").setPath("F://file").build();
        fileQueue.createProduction();
//        fileQueue.createGroup("test1", FileQueue.GrowMode.CONTINUE_OFFSET);
//        Consumption<Map> test1 = fileQueue.getConsumption("test");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            List<Test1> tests = new ArrayList<>();
            tests.add(new Test1(i));
////            fileQueue.put(new Test("name" + i, tests));
            Map map = new HashMap();
            map.put("key" + i, new Test("name" + i, tests));
//            ProtostuffUtils.serializer(new CollectionEntry<>(map));
//            fileQueue.put( new Test("name" + i));
            fileQueue.put(map);
        }
        System.out.println(System.currentTimeMillis() - start);
        Map<String, Test> test;
        int i = 0;
        start = System.currentTimeMillis();
//        while ((test = test1.poll()) != null) {
////            System.out.println(JSONObject.toJSONString(test));
//            System.out.println(JSONObject.toJSONString(test.get("key" + i).getTest1s()));
//            i ++;
//        }
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(i);
    }

    private static void test() throws Exception {
        FileQueue<Test> testFilePlusQueue = FileQueue.ordinary(Test.class, "test");
        long start = System.currentTimeMillis();
        for (int i = 128; i < 1000; i++) {
            testFilePlusQueue.put(new Test("name" + i));
        }
        System.out.println(System.currentTimeMillis() - start);
        Test test;
        int i = 0;
        System.out.println(testFilePlusQueue.size());
        start = System.currentTimeMillis();
        while ((test = testFilePlusQueue.poll()) != null) {
            System.out.println(JSONObject.toJSONString(test));
            i ++;
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    private static void test1() throws IOException {
        RandomAccessFile accessFileOffsetList = new RandomAccessFile("./queue/test" + File.separator + "0000000000000000000.offsetList", "rw");
        FileChannel fileChannelOffsetList = accessFileOffsetList.getChannel();
        MappedByteBuffer map = fileChannelOffsetList.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 10);
//        map.putInt()
//        map.put()
//        map.putLong(85);
//        map.putLong(25);
//        map.flip();

        System.out.println(map.getLong());
        long l = 0;
        int i = 0;
        while ((l = map.getLong()) > 0) {
            if (i > 50384) {
                return;
            }
            System.out.println(l);
            i++;
        }
//        System.out.println(map.getLong());
//        System.out.println(map.getLong());
//        System.out.println(map.getLong());
    }

    private static void test6() throws NoSuchFieldException, IllegalAccessException {
        boolean a = ArrayList.class.isAssignableFrom(List.class);
//        System.out.println(MappedByteBufferUtil.isCollection(ArrayList.class));
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            new Test("name" + i);
//            JSONObject.toJSONString(new Test("name" + i));
            ProtostuffUtils.serializer(new Test("name" + i));
        }
        long end = System.currentTimeMillis();
        System.out.println("序列化:" + (end - start));
//        for (int i = 0; i < 10000000; i++) {
////            JSONObject.parseObject("{\"name\":\"name" + i + "\"}", Test.class);
////            ProtostuffUtils.deserializer(("{\"name\":\"name" + i + "\"}").getBytes(), Test.class);
//       }
//        System.out.println(System.currentTimeMillis() - end);
    }


    private static void test4() throws Exception {
        FileQueue<Test> fileQueue = FileQueueBuilder.create(Test.class, "test").setType(FileQueue.IS_ALL).setGroupName("test")
                .setQueueModel(FileQueue.QueueModel.SUBSCRIBE).build();

        List<Test> tests = new ArrayList<>(10000000);
        BlockingQueue<Test> queue = new LinkedBlockingQueue<>();
        int count = 10000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            fileQueue.put(new Test("name" + i));
        }
        long end = System.currentTimeMillis() - start;
        System.out.println("文件队列耗时:" + end);
//        start = System.currentTimeMillis();
//        for (int i = 0; i < count; i++) {
//            tests.add(new Test("name" + i));
//        }
//        end = System.currentTimeMillis() - start;
//        System.out.println("list耗时:" + end);
//        start = System.currentTimeMillis();
//        for (int i = 0; i < count; i++) {
//            queue.add(new Test("name" + i));
//        }
//        end = System.currentTimeMillis() - start;
//        System.out.println("阻塞队列耗时:" + end);
//        int i = 1000;
//        for (;;) {
//            fileQueue.put(new Test("name" + i ++));
//            Thread.sleep(1000);
//        }
        Test test = null;
        Consumption<Test> test1 = fileQueue.getConsumption("test");
        start = System.currentTimeMillis();
        while (test1.poll() != null) {

        }
        end = System.currentTimeMillis() - start;
        System.out.println("读取:" + end);
    }

    private static void test5() throws Exception {
        FileQueue<Test> fileQueue = FileQueueBuilder.create(Test.class, "test").setGroupName("test")
                .setQueueModel(FileQueue.QueueModel.SUBSCRIBE).build();
        Consumption<Test> consumption = fileQueue.createFirstGroup("test", false);
        Test test;
        int i = 0;
        while ((test = consumption.poll()) != null) {
            System.out.println(JSONObject.toJSONString(test));
            i ++;
        }
        for( ; ;) {
            Test poll = consumption.poll();
            System.out.println(JSONObject.toJSONString(poll));
            i ++;
            Thread.sleep(800);
        }
//        System.out.println(i);
    }

}
