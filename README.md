# fileQueue

#### 简单介绍
1.一个高性能,可靠,同步的本地持久化队列；可以做到重启JVM、重启服务器、或者强制KILL进程时,队列里的数据不丢失,下次启动应用时,仍可以继续消费;

2.每秒的效率可达到几百万(使用fastjson:写一千万对象3秒,读一千万对象2秒以内,使用protostuff:写一千万对象两秒以内，读一千万对象1.5秒以内).

3.支持发布订阅,一台机器可以多进程同时使用(一个topic(主题),写只能一个进程,一个group(消费组),只能一个进程)

4.有问题可以一起探讨完善(QQ:314727247)

#### 架构设计
1. 使用零拷贝(MMAP)和文件顺序读写

2. 文件设计，总共四种类型文件
    1.log文件,主要用于存储数据的文件,每一条消息会序列化后存储到这个文件中
    2.offset文件,主要用于存储每条数据在文件中的偏移量
    3.write文件，主要是生产者对应的一些信息(写文件的信息)
    4.read文件，主要是消费者对应的一些信息(读文件的信息)




#### 安装教程

1.  直接打包放入项目中就可以使用，简单方便

#### 使用说明

1.  普通队列模式使用
     
      	
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

2.  发布订阅模式使用

        FileQueue<Test> subscribe = FileQueue.subscribe(Test.class, "subscribe", "test1");
        Production<Test> production = subscribe.getProduction();
        int count = 10000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            production.put(new Test("name" + i));
        }
        long end = System.currentTimeMillis();
        System.out.println("subscribe,生产:" + count + "条,数据耗时:" + (end - start) + "毫秒");
        Consumption<Test> test1 = subscribe.getConsumption("test1");
        int i = 0;
        start = System.currentTimeMillis();
        while (test1.poll() != null) {
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

3.  可以通过构建者模式构建
        
        FileQueue<Test> fileQueue = FileQueueBuilder.create(Test.class, "test").setType(FileQueue.IS_ALL).setGroupName("test")
                .setQueueModel(FileQueue.QueueModel.SUBSCRIBE).build();
        Production<Test> production = fileQueue.getProduction();
        int count = 10000000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            production.put(new Test("name" + i));
        }
        long end = System.currentTimeMillis();
        System.out.println("subscribe,生产:" + count + "条,数据耗时:" + (end - start) + "毫秒");
        Consumption<Test> test1 = fileQueue.createGroup("test1", FileQueue.GrowMode.FIRST_OFFSET);
        int i = 0;
        start = System.currentTimeMillis();
        while (test1.poll() != null) {
            i ++;
        }
        end = System.currentTimeMillis();
        System.out.println("test1消费:" + i + "条,数据耗时:" + (end - start) + "毫秒");
        i = 0;
        Consumption<Test> test2 = fileQueue.createGroup("test2", FileQueue.GrowMode.FIRST_OFFSET);
        start = System.currentTimeMillis();
        while (test2.poll() != null) {
            i ++;
        }
        end = System.currentTimeMillis();
        System.out.println("test2消费:" + i + "条,数据耗时:" + (end - start) + "毫秒");

