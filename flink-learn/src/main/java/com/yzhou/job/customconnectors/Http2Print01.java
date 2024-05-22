package com.yzhou.job.customconnectors;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Http2Print01 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(5000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册表
        // kafka
        String createKafkaTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/source/httpSource01.sql");
        TableResult kafkaTableResult = tableEnv.executeSql(createKafkaTableSql);
        // mysql
        String createMySQLTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/sink/print03.sql");;
        TableResult mysqlTableResult = tableEnv.executeSql(createMySQLTableSql);

        // 转 Table
        String queryKafkaTableSql = "select * from http_source";
        Table kafkaTable = tableEnv.sqlQuery(queryKafkaTableSql);

        // 创建 kafka 临时表
        // tableEnv.createTemporaryView("kafka", kafkaTable);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("sink_table",kafkaTable);

        // 执行
        statementSet.execute();
    }
}


