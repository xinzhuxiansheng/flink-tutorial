package com.yzhou.job.streamencode;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

public class AppendOnlySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        /*
        CREATE TABLE orders(
            order_id    BIGINT,
            price       DECIMAL(10,2),
            order_time  TIMESTAMP
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '1'
        )
         */
        String inTableSql = "CREATE TABLE orders(\n" +
                "    order_id    BIGINT,\n" +
                "    price       DECIMAL(10,2),\n" +
                "    order_time  TIMESTAMP\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '1'\n" +
                ")";
        tEnv.executeSql(inTableSql);

        Table resTable = tEnv.sqlQuery("SELECT * FROM orders");
        DataStream<Row> resStream = tEnv.toChangelogStream(resTable,
                Schema.newBuilder().build(),
                ChangelogMode.insertOnly());

        resStream.print();
        env.execute("AppendOnlySQL");
    }
}
