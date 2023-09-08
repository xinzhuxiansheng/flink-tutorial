package com.yzhou.common.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLUtil {

    public static Connection getConnection() throws Exception {

        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://hadoop000:13306/ruozedata", "root","000000");

    }


    public static void close(AutoCloseable closeable) {
        if(null != closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
