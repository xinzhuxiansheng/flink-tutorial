package com.yzhou.job.streamencode;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class RetractSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        String inTableSql = "CREATE TABLE orders(\n" +
                "    order_id    BIGINT,\n" +
                "    price       DECIMAL(10,2),\n" +
                "    order_time  TIMESTAMP\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '1',\n" +
                "    'fields.order_id.min' = '100',\n" +
                "    'fields.order_id.max' = '105'\n" +
                ")";
        tEnv.executeSql(inTableSql);

        Table resTable = tEnv.sqlQuery("SELECT order_id,COUNT(*) AS cnt FROM orders GROUP BY order_id");
        DataStream<Row> resStream = tEnv.toChangelogStream(resTable,
                Schema.newBuilder().build(),
                ChangelogMode.all());

//        private static final ChangelogMode ALL =
//                ChangelogMode.newBuilder()
//                        .addContainedKind(RowKind.INSERT)
//                        .addContainedKind(RowKind.UPDATE_BEFORE)
//                        .addContainedKind(RowKind.UPDATE_AFTER)
//                        .addContainedKind(RowKind.DELETE)
//                        .build();

        resStream.print();
        env.execute("RetractSQL");
    }
}
