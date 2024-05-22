package com.yzhou.udf.scalarfunction;

import org.apache.flink.table.functions.ScalarFunction;

/*
    拼接字符串
 */
public class AddSuffix extends ScalarFunction {
    private final String suffix = "_suffix";

    public AddSuffix(){

    }

//    public AddSuffix(String suffix) {
//        this.suffix = suffix;
//    }

    public String eval(String s) {
        return s + suffix;
    }
}
