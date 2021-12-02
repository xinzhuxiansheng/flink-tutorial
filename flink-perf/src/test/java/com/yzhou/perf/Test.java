package com.yzhou.perf;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yzhou
 * @date 2021/11/26
 */
public class Test {

    public static void main(String[] args) {
        Map<String,Object> a = new HashMap<>();
        a.put("aa","22");

        int b = (int)a.get("aa");
        System.out.println(b);
    }
}
