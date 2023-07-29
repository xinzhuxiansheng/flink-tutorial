package com.yzhou.common.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间处理工具类
 */
public class DateUtils {

    /**
     *
     * @param ts
     * @param formats  yyyyMMdd
     * @return 将long类型的时间戳转成指定格式的日期
     */
    public static String ts2Date(long ts, String formats) {

//        return new SimpleDateFormat(formats).format(new Date(ts * 1000));
        return new SimpleDateFormat(formats).format(new Date(ts));
    }

    public static void main(String[] args) {
        System.out.println(ts2Date(1683481755, "yyyyMMdd"));
    }

}
