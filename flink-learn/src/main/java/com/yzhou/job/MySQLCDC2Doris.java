package com.yzhou.job;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySQLCDC2Doris {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(5000);
        // 设置 checkpoint 模式（例如，EXACTLY_ONCE 或 AT_LEAST_ONCE）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置 checkpoint 的存储路径
        String checkpointPath = "file:///D:\\TMP\\FlinkCheckPointPath";
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String parentPath = "D:\\Code\\Java\\flink-tutorial\\flink-learn\\src\\main\\resources\\";

        // 注册表
        // source
        String sourceSqlPath = "mysqlcdc2doris\\01mysqlcdcCreateTable.sql";
        String createSourceTableSql = FileUtil.readFile(parentPath + sourceSqlPath);
        TableResult sourceTableResult = tableEnv.executeSql(createSourceTableSql);
        // sink
        String sinksqlPath = "mysqlcdc2doris\\01DorisCreateTable.sql";
        String createSinkTableSql = FileUtil.readFile(parentPath + sinksqlPath);
        ;
        TableResult sinkTableResult = tableEnv.executeSql(createSinkTableSql);

        // 转 Table
        String querySourceTableSql = "select * from mysqlcdc_source";
        Table sourceTable = tableEnv.sqlQuery(querySourceTableSql);

        // 创建 kafka 临时表
        // tableEnv.createTemporaryView("kafka", kafkaTable);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("doris_sink", sourceTable);

        // 执行
        statementSet.execute();
    }
}
