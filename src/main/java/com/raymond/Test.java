package com.raymond;


import java.util.List;

/**
 * 测试表
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2019-12-16 17:23
 */

public class Test {

    private String name;

    private List<Test1> test1s;


    public Test() {
    }

    public Test(String name) {
        this.name = name;
    }

    public Test(String name, List<Test1> test1s) {
        this.name = name;
        this.test1s = test1s;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Test1> getTest1s() {
        return test1s;
    }

    public void setTest1s(List<Test1> test1s) {
        this.test1s = test1s;
    }
}
