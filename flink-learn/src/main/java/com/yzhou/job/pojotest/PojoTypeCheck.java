package com.yzhou.job.pojotest;

import org.apache.flink.api.java.typeutils.TypeExtractor;

public class PojoTypeCheck {
    public static void main(String[] args) {
        System.out.println("具有私有成员变量的 POJO 对象："+ TypeExtractor.createTypeInfo(UserPoPrivateField.class));
        System.out.println("具有公有成员变量的 POJO 对象："+ TypeExtractor.createTypeInfo(UserPoPublicField.class));
        System.out.println("实现了接口的 POJO 对象："+ TypeExtractor.createTypeInfo(UserPoImplement.class));
        System.out.println("具有私有成员变量但没有 Getter 和 Setter 的 POJO 对象："+ TypeExtractor.createTypeInfo(UserPoPrivateFieldWithoutGetSet.class));
    }
}
