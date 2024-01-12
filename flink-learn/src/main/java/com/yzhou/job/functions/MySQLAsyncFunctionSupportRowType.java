package com.yzhou.job.functions;

import com.alibaba.druid.pool.DruidDataSource;
import com.yzhou.job.util.MySQLUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Table API -> DataStream API -> Async IO MySQL -> Print()
 * 注意要重新构建 Row 才能打印出新数据
 */
public class MySQLAsyncFunctionSupportRowType extends RichAsyncFunction<Row, Row> {

    private transient DruidDataSource dataSource;
    private transient ExecutorService executorService;

    private int maxConnection;

    public MySQLAsyncFunctionSupportRowType(int maxConnection) {
        this.maxConnection = maxConnection;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        executorService = Executors.newFixedThreadPool(maxConnection);
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
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
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        Future<Map<String, String>> future = executorService.submit(new Callable<Map<String, String>>() {
            @Override
            public Map<String, String> call() throws Exception {
                return queryFromMySQL(input);
            }
        });

        CompletableFuture.supplyAsync(new Supplier<Map<String, String>>() {
            @Override
            public Map<String, String> get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    return null;
                }
            }
        }).thenAccept((Map<String, String> dbResult) -> {
            Row mergedRow = mergeRowAndMap(input,dbResult);
            resultFuture.complete(Collections.singleton(mergedRow));
        });
    }

    private Map<String, String> queryFromMySQL(Row input) throws Exception {
        String sql = "select id,province,city from address where id = ? ";
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String province = null;
        String city = null;
        try {
            connection = dataSource.getConnection();
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, input.getField(2).toString()); // addressId
            rs = pstmt.executeQuery();
            while (rs.next()) {
                province = rs.getString("province");
                city = rs.getString("city");
            }
        } finally {
            MySQLUtils.close(rs);
            MySQLUtils.close(pstmt);
            MySQLUtils.close(connection);
        }
        Map<String, String> result = new HashMap<>();
        result.put("province", province);
        result.put("city", city);
        return result;
    }

    private Row mergeRowAndMap(Row input, Map<String, String> map) {
        // 计算新Row的总字段数：原始Row的字段数 + HashMap的大小
        Row mergedRow = new Row(input.getArity() + map.size());
        // 复制原始Row的字段
        for (int i = 0; i < input.getArity(); i++) {
            mergedRow.setField(i, input.getField(i));
        }
        // 添加HashMap的值
        int index = input.getArity(); // 从原始Row的末尾开始
        for (String key : map.keySet()) {
            mergedRow.setField(index, map.get(key));
            index++;
        }
        return mergedRow;
    }
}

