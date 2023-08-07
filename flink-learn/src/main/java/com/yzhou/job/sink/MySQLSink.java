package com.yzhou.job.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

import java.sql.*;

public class MySQLSink extends RichSinkFunction<Tuple2<String, Long>> {
    private static Logger logger = Logger.getLogger(MySQLSink.class);
    private Connection conn;
    private PreparedStatement insertPS;
    private PreparedStatement updatePS;
    private String dbHost;
    private String dbPort;
    private String dbName;
    private String table;

    public MySQLSink(String dbHost, String dbPort, String dbName, String table) {
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.table = table;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://"+dbHost+":"+dbPort+"/" + dbName + "?characterEncoding=utf8&connectTimeout=1000&socketTimeout=3000&autoReconnect=true&useUnicode=true&useSSL=false&serverTimezone=UTC";
        //建立mysql连接
        conn = DriverManager.getConnection(url, "root", "12345678");
        String sql = "INSERT INTO "+table+" (`word`, `cnt`) VALUES(?,?);";
        insertPS = conn.prepareStatement(sql);

        String sql2 = "UPDATE "+table+" set cnt = ? where word = ?;";
        updatePS = conn.prepareStatement(sql2);

        System.out.println("自定义sink，open数据库链接 =====");
        logger.info("*** 自定义sink，open数据库链接 =====");
    }

    @Override
    public void close() throws Exception {
        if(insertPS != null){
            insertPS.close();
        }
        if(updatePS != null){
            updatePS.close();
        }
        if(conn != null){
            conn.close();
        }
        System.out.println("自定义sink，close数据库连接 =====");
        logger.info("*** 自定义sink，close数据库连接 =====");
    }

    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        String sql = "select count(*) from "+table+" where word = '" + value.f0 + "'";
        System.out.println(sql);
        logger.info("*** " + sql);
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        long cnt = 0;
        while (resultSet.next()) {
            cnt = resultSet.getLong(1);
        }
        resultSet.close();
        statement.close();

        if (cnt > 0) {
            System.out.println("update value=" + value);
            updatePS.setLong(1,value.f1);
            updatePS.setString(2,value.f0);
            updatePS.executeUpdate();
        } else {
            System.out.println("insert value=" + value);
            insertPS.setString(1,value.f0);
            insertPS.setLong(2,value.f1);
            insertPS.executeUpdate();
        }
    }
}