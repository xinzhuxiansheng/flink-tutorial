package com.yzhou.udf.scalarfunction;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

public class PrintTIMESTAMPLTZ extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(PrintTIMESTAMPLTZ.class);

    public PrintTIMESTAMPLTZ() {}
    public String eval(LocalDateTime procTime) {
        // 获取并打印时区
        TimeZone tz = TimeZone.getDefault();
        logger.info("yzhou Current JVM Time Zone: " + tz.getID());

        // 获取并打印当前日期和时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(tz); // 确保日期/时间是在当前时区
        Date now = new Date();
        logger.info("yzhou Current Date and Time: " + sdf.format(now));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedTime = procTime.format(formatter);
        logger.info("yzhou Processed time: " + formattedTime);
        return formattedTime;
    }
}
