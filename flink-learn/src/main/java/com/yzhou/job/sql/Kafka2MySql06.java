package com.yzhou.job.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Kafka2MySql06 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        env.enableCheckpointing(5000);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String scanStartupMode = parameterTool.get("scan.startup.mode");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册表
        // kafka
        String createKafkaTableSql = "CREATE TABLE\n"
            + "  `yzhoujsontp01` (\n"
            + "    LOG_TIMESTAMP STRING,\n"
            + "    d_timestamp AS TO_TIMESTAMP (LOG_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSSSSS'),\n"
            + "    WATERMARK FOR d_timestamp AS d_timestamp\n"
            + "  )\n"
            + "WITH\n"
            + "  (\n"
            + "    'properties.bootstrap.servers' = 'vm01:9092',\n"
            + "    'connector' = 'kafka',\n"
            + "    'json.ignore-parse-errors' = 'false',\n"
            + "    'format' = 'json',\n"
            + "    'topic' = 'yzhoujsontp01',\n"
            + "    'properties.group.id' = 'testGroup02',\n"
            + "    'scan.startup.mode' = 'earliest-offset',\n"
            + "    'json.fail-on-missing-field' = 'false'\n"
            + "  )";
        TableResult kafkaTableResult = tableEnv.executeSql(createKafkaTableSql);
        // mysql
        String createMySQLTableSql = "  CREATE TABLE\n"
            + "  `st_output01` (\n"
            + "    log_date STRING,\n"
            + "    count_per_day BIGINT,\n"
            + "    PRIMARY KEY (log_date) NOT ENFORCED\n"
            + "  )\n"
            + "WITH\n"
            + "  (\n"
            + "    'password' = '123456',\n"
            + "    'connector' = 'jdbc',\n"
            + "    'table-name' = 'st_output01',\n"
            + "    'sink.parallelism' = '1',\n"
            + "    'url' = 'jdbc:mysql://vm01:3306/yzhou_test',\n"
            + "    'username' = 'root'\n"
            + "  )";
        TableResult mysqlTableResult = tableEnv.executeSql(createMySQLTableSql);

        tableEnv.executeSql(
                "INSERT INTO st_output01\n"
                    + "SELECT\n"
                    + "    DATE_FORMAT(TO_TIMESTAMP(LOG_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS log_date,\n"
                    + "    count(*) AS count_per_day\n"
                    + "FROM yzhoujsontp01\n"
                    + "GROUP BY\n"
                    + "    TUMBLE(PROCTIME(),INTERVAL '1' MINUTE),\n"
                    + "    DATE_FORMAT(TO_TIMESTAMP(LOG_TIMESTAMP,'yyyy-MM-dd HH:mm:ss.SSSSSS'),'yyyy-MM-dd');"
        );
    }
}


