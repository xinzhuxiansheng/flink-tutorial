package com.yzhou.udf.scalarfunction;

import org.apache.flink.table.functions.ScalarFunction;

/*
    拼接 两个字段
 */
public class Concatenate2Fields extends ScalarFunction {
    public Concatenate2Fields() {}
    public String eval(String s1,String s2) {
        return s1+","+s2;
    }
}
