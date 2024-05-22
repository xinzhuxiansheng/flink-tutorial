package com.yzhou.job.udf;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class Kafka2MySqlWithUDF {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(5000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册表
        // kafka
        String createKafkaTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/kafka2mysql/03KafkaCreateTable.sql");
        TableResult kafkaTableResult = tableEnv.executeSql(createKafkaTableSql);
        // mysql
        String createMySQLTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/kafka2mysql/03mysqlCreateTable.sql");;
        TableResult mysqlTableResult = tableEnv.executeSql(createMySQLTableSql);

        // 注册UDF
        tableEnv.createTemporarySystemFunction("AddSuffix", new AddSuffix("_suffix"));

        // 转 Table
        String queryKafkaTableSql = "select id,AddSuffix(name),address,ext_field01 from kafka_source";
        Table kafkaTable = tableEnv.sqlQuery(queryKafkaTableSql);

        // 创建 kafka 临时表
        // tableEnv.createTemporaryView("kafka", kafkaTable);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("mysql_sink",kafkaTable);

        // 执行
        statementSet.execute();
    }

    public static class AddSuffix extends ScalarFunction {
        private final String suffix;

        public AddSuffix(String suffix) {
            this.suffix = suffix;
        }

        public String eval(String s) {
            return s + suffix;
        }
    }
}
