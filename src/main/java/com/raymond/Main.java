package com.raymond;

import com.alibaba.fastjson.JSONObject;
import com.raymond.queue.Consumption;
import com.raymond.queue.FileQueue;
import com.raymond.queue.Production;
import com.raymond.queue.collection.CollectionEntry;
import com.raymond.queue.utils.FileQueueBuilder;
import com.raymond.queue.utils.MappedByteBufferUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

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
        test2();
//        test3();
//        test1();
//        test6();
//        test();
//        Thread.sleep(Integer.MAX_VALUE);

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
        FileQueue<Map> fileQueue = FileQueue.subscribe(Map.class, "test", "test");
//        fileQueue.createGroup("test1", FileQueue.GrowMode.CONTINUE_OFFSET);
        Consumption<Map> test1 = fileQueue.getConsumption("test");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            List<Test1> tests = new ArrayList<>();
            tests.add(new Test1(i));
////            fileQueue.put(new Test("name" + i, tests));
            Map map = new HashMap();
            map.put("key" + i, new Test("name" + i, tests));
//            fileQueue.put( new Test("name" + i));
            fileQueue.put(map);
        }
        System.out.println(System.currentTimeMillis() - start);
        Map test;
        int i = 0;
        start = System.currentTimeMillis();
        while ((test = test1.poll()) != null) {
//            System.out.println(JSONObject.toJSONString(test));
            i ++;
        }
        System.out.println(System.currentTimeMillis() - start);
        System.out.println(i);
    }

    private static void test() throws Exception {
        FileQueue<Test> testFilePlusQueue = FileQueue.ordinary(Test.class, "test");
        long start = System.currentTimeMillis();
//        for (int i = 128; i < 1000; i++) {
//            testFilePlusQueue.put(new Test("name" + i));
//        }
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
        RandomAccessFile accessFileOffsetList = new RandomAccessFile("./queue/test" + File.separator + "queue.write", "rw");
        FileChannel fileChannelOffsetList = accessFileOffsetList.getChannel();
        MappedByteBuffer map = fileChannelOffsetList.map(FileChannel.MapMode.READ_WRITE, 44, 32);
//        map.putInt()
//        map.put()
//        map.putLong(85);
//        map.putLong(25);
//        map.flip();
        System.out.println(map.getLong());
        System.out.println(map.getLong());
        System.out.println(map.getLong());
        System.out.println(map.getLong());
    }

    private static void test6() throws NoSuchFieldException, IllegalAccessException {
        boolean a = ArrayList.class.isAssignableFrom(List.class);
        System.out.println(MappedByteBufferUtil.isCollection(ArrayList.class));
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
//            new Test("name" + i);
//            JSONObject.toJSONString(new Test("name" + i));
//            ProtostuffUtils.serializer(new Test("name" + i));
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        for (int i = 0; i < 10000000; i++) {
//            JSONObject.parseObject("{\"name\":\"name" + i + "\"}", Test.class);
//            ProtostuffUtils.deserializer(("{\"name\":\"name" + i + "\"}").getBytes(), Test.class);
       }
        System.out.println(System.currentTimeMillis() - end);
    }


    private static void test4() throws Exception {
        FileQueue<Test> fileQueue = FileQueueBuilder.create(Test.class, "test").setType(FileQueue.IS_PRODUCTION)
                .setQueueModel(FileQueue.QueueModel.SUBSCRIBE).build();
        for (int i = 0; i < 1000; i++) {
            fileQueue.put(new Test("name" + i));
        }
        int i = 1000;
        for (;;) {
            fileQueue.put(new Test("name" + i ++));
            Thread.sleep(1000);
        }
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
