package com.yzhou.job.tableapiinterconversiondatastreamapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/*
    Table API 互转 DataStream API
 */
public class TableAPIInterconverDataStream01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");

        // 1. 使用 StreamTableEnvironment::fromDataStream API 将 DataStream 转为 Table
        Table inputTable = tableEnv.fromDataStream(dataStream);

        // 将 Table 注册为一个临时表
        tableEnv.createTemporaryView("InputTable", inputTable);

        // 然后就可以在这个临时表上做一些自定义的查询了
        Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");

        // 2. 也可以使用 StreamTableEnvironment::toDataStream 将 Table 转为 DataStream
        // 注意：这里只能转为 DataStream<Row>，其中的数据类型只能为 Row
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        // 将 DataStream 结果打印到控制台
        resultStream.print();
        env.execute();
    }
}
