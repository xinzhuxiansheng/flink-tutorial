package com.yzhou.job;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Kafka2Doris {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(5000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置 checkpoint 模式（例如，EXACTLY_ONCE 或 AT_LEAST_ONCE）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置 checkpoint 的存储路径
        String checkpointPath = "file:///D:\\TMP\\FlinkCheckPointPath";
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);

        String parentPath = "D:\\Code\\Java\\flink-tutorial\\flink-learn\\src\\main\\resources\\";

        // 注册表
        // source
        String sourceSqlPath = "kafka2doris\\01KafkaCreateTable.sql";
        String createKafkaTableSql = FileUtil.
                readFile(parentPath + sourceSqlPath);
        TableResult kafkaTableResult = tableEnv.executeSql(createKafkaTableSql);
        // sink
        String sinkSqlPath = "kafka2doris\\01DorisCreateTable.sql";
        String createMySQLTableSql = FileUtil.
                readFile(parentPath + sinkSqlPath);
        TableResult mysqlTableResult = tableEnv.executeSql(createMySQLTableSql);

        // 转 Table
        String queryKafkaTableSql = "select *,if(op_type='D',1,0) as __DORIS_DELETE_SIGN__  from kafka_source";
        Table kafkaTable = tableEnv.sqlQuery(queryKafkaTableSql);

        // 创建 kafka 临时表
        // tableEnv.createTemporaryView("kafka", kafkaTable);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("doris_sink", kafkaTable);

        // 执行
        statementSet.execute();
    }
}
