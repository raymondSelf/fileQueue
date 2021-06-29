# fileQueue

#### 介绍
1. 一个高性能,可靠,同步的本地持久化队列；可以做到重启JVM、重启服务器、或者强制KILL进程时,队列里的数据不丢失,下次启动应用时,仍可以继续消费;
2. 每秒的效率可达到几百万(使用fastjson:写一千万对象3秒,读一千万对象2秒以内,使用protostuff:写一千万对象两秒以内，读一千万对象1.5秒以内)。
3. 支持发布订阅,一台机器可以多进程同时使用(一个topic(主题),写只能一个进程,一个group(消费组),只能一个进程)
4. 有问题可以一起探讨完善(QQ:314727247)。觉得还可以的话点个星星呗

#### 软件架构
软件架构说明


#### 安装教程

1.  xxxx
2.  xxxx
3.  xxxx

#### 使用说明

1.  直接打包放入项目中就可以使用
2.  xxxx
3.  xxxx

#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


#### 特技

1.  使用 Readme\_XXX.md 来支持不同的语言，例如 Readme\_en.md, Readme\_zh.md
2.  Gitee 官方博客 [blog.gitee.com](https://blog.gitee.com)
3.  你可以 [https://gitee.com/explore](https://gitee.com/explore) 这个地址来了解 Gitee 上的优秀开源项目
4.  [GVP](https://gitee.com/gvp) 全称是 Gitee 最有价值开源项目，是综合评定出的优秀开源项目
5.  Gitee 官方提供的使用手册 [https://gitee.com/help](https://gitee.com/help)
6.  Gitee 封面人物是一档用来展示 Gitee 会员风采的栏目 [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)
