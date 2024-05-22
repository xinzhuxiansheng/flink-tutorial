package com.yzhou.job.pojotest;

/**
 * 具有私有成员变量的 POJO 对象
 */
public class UserPoPrivateField {
    private String name;

    public UserPoPrivateField() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
