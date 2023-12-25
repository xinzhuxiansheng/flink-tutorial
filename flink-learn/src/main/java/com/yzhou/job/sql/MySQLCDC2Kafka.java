package com.yzhou.job.sql;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySQLCDC2Kafka {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(5000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册表
        // kafka
        String createSourceTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/mysqlcdc2kafka/01mysqlcdcCreateTable.sql");
        TableResult sourceTableResult = tableEnv.executeSql(createSourceTableSql);
        // mysql
        String createSinkTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/mysqlcdc2kafka/01KafkaCreateTable.sql");;
        TableResult sinkTableResult = tableEnv.executeSql(createSinkTableSql);

        // 转 Table
        String querySourceTableSql = "select * from mysqlcdc_source";
        Table sourceTable = tableEnv.sqlQuery(querySourceTableSql);

        // 创建 kafka 临时表
        // tableEnv.createTemporaryView("kafka", kafkaTable);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("kafka_sink",sourceTable);

        // 执行
        statementSet.execute();
    }
}
