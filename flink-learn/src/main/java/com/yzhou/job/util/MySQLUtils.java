package com.yzhou.job.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLUtils {

    public static Connection getConnection() throws Exception {

        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://localhost:3306/yzhou_test", "root","12345678");
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
