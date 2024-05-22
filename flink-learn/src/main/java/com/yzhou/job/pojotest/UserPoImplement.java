package com.yzhou.job.pojotest;

/**
 * 实现了接口的 POJO 对象
 */
public class UserPoImplement implements Person{

    private String name;
    @Override
    public void write() {

    }

    public UserPoImplement() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
