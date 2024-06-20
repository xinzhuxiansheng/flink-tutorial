package com.yzhou.job.app;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.types.Row;

public class TableAPI2DataStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        String sql = FileUtil.readFile("D:\\Code\\Java\\flink-tutorial\\flink-learn\\src\\main\\resources\\mysql\\jdbc02.sql");

        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build();

        // 创建StreamTableEnvironment实例
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        //指定方言 (选择使用SQL语法还是HQL语法)
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tableEnv.executeSql(sql);

        String selectSql = "SELECT * FROM yzhou_test01";
        Table table =  tableEnv.sqlQuery(selectSql);

        DataStream<Row> dataStream =  tableEnv.toDataStream(table, Row.class);

        dataStream.print();
        env.execute("table api 2 datastream");
    }
}
