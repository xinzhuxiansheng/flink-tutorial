package com.yzhou.job.functions;

import com.alibaba.druid.pool.DruidDataSource;
import com.yzhou.job.util.MySQLUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * 异步 I/O
 */
public class MySQLAsyncFunction extends RichAsyncFunction<String, Tuple2<String,String>> {

    private transient DruidDataSource dataSource;
    private transient ExecutorService executorService;

    private int maxConnection;

    public MySQLAsyncFunction(int maxConnection) {
        this.maxConnection = maxConnection;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        executorService = Executors.newFixedThreadPool(maxConnection);
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("12345678");
        dataSource.setUrl("jdbc:mysql://localhost:3306/yzhou_test");
        dataSource.setMaxActive(maxConnection);
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
        executorService.shutdown();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        Future<String> future = executorService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return queryFromMySQL(input);
            }
        });

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    return null;
                }
            }
        }).thenAccept((String dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(input, dbResult)));
        });
    }

    private String queryFromMySQL(String input) throws Exception {

        String sql = "select id,name from course where id=?";
        String result = null;

        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try{
            connection = dataSource.getConnection();
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, input);
            rs = pstmt.executeQuery();
            while(rs.next()) {
                result = rs.getString("name");
            }

        } finally {
            MySQLUtils.close(rs);
            MySQLUtils.close(pstmt);
            MySQLUtils.close(connection);
        }

        return result;
    }
}

