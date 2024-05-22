package com.yzhou.job.sql;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataGen2MySql01 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(5000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册表
        // kafka
        String createKafkaTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/source/datagenSource02.sql");
        TableResult kafkaTableResult = tableEnv.executeSql(createKafkaTableSql);
        // mysql
        String createMySQLTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/sink/mysqlSink01.sql");
        ;
        TableResult mysqlTableResult = tableEnv.executeSql(createMySQLTableSql);

        // 转 Table
        String queryKafkaTableSql = "select bid,proctime,nowtime,localtime01 from source_table";
        Table kafkaTable = tableEnv.sqlQuery(queryKafkaTableSql);

        // 创建 kafka 临时表
        tableEnv.createTemporaryView("kafka", kafkaTable);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("mysql_sink", kafkaTable);

        // 执行
        statementSet.execute();
    }
}


