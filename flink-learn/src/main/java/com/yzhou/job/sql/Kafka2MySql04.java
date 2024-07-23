package com.yzhou.job.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Kafka2MySql04 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        env.enableCheckpointing(5000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册表
        // kafka
        String createKafkaTableSql = "CREATE TABLE\n" +
                "  `yzhoujsontp13` (\n" +
                "    `id` INT COMMENT '',\n" +
                "    `name` STRING COMMENT '',\n" +
                "    `address` STRING COMMENT '',\n" +
                "    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL" +
                "  )\n" +
                "WITH\n" +
                "  (\n" +
                "    'properties.bootstrap.servers' = '192.168.0.201:9092',\n" +
                "    'connector' = 'kafka',\n" +
                "    'json.ignore-parse-errors' = 'false',\n" +
                "    'format' = 'json',\n" +
                "    'topic' = 'yzhoujsontp01',\n" +
                "    'properties.group.id' = 'testGroup-gid072102',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'json.fail-on-missing-field' = 'false'\n" +
                "  )";
        TableResult kafkaTableResult = tableEnv.executeSql(createKafkaTableSql);
        // mysql
        String createMySQLTableSql = "CREATE TABLE\n" +
                "  `yzhou_test02` (\n" +
                "    `id` INT NOT NULL COMMENT '',\n" +
                "    `name` STRING NOT NULL COMMENT '',\n" +
                "    `address` STRING COMMENT '',\n" +
                "    kafka_offset BIGINT COMMENT 'Kafka Offset'," +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                "  )\n" +
                "WITH\n" +
                "  (\n" +
                "    'password' = '123456',\n" +
                "    'connector' = 'jdbc',\n" +
                "    'table-name' = 'yzhou_test03',\n" +
                "    'sink.parallelism' = '1',\n" +
                "    'url' = 'jdbc:mysql://192.168.0.202:3306/yzhou_test',\n" +
                "    'username' = 'root'\n" +
                "  )";
        TableResult mysqlTableResult = tableEnv.executeSql(createMySQLTableSql);

        tableEnv.executeSql(
                " INSERT INTO yzhou_test02 " +
                        " SELECT * FROM yzhoujsontp13 "
        );
    }
}


