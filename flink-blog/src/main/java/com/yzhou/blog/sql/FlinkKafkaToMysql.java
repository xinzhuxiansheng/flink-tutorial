package com.yzhou.blog.sql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkKafkaToMysql {
    public static void main(String[] args) {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 定义 Kafka 源表
        tableEnv.executeSql(
                "CREATE TABLE kafka_source (" +
                        "    name STRING, " +
                        "    money INT, " +
                        "    date_str STRING " +
                        ") WITH (" +
                        "    'connector' = 'kafka', " +
                        "    'topic' = 'yzhoujsontp01', " +
                        "    'properties.bootstrap.servers' = '192.168.0.201:9092', " +
                        "    'properties.group.id' = 'gid11191712_01', " +
                        "    'format' = 'json', " +
                        "    'scan.startup.mode' = 'latest-offset'" +
                        ")"
        );

        // 定义 MySQL Sink 表
        tableEnv.executeSql(
                "CREATE TABLE mysql_sink (" +
                        "    name STRING, " +
                        "    total_money INT, " +
                        "    date_str STRING, " +
                        "    PRIMARY KEY (name) NOT ENFORCED " +
                        ") WITH (" +
                        "    'connector' = 'jdbc', " +
                        "    'url' = 'jdbc:mysql://192.168.0.201:3306/yzhou_test', " +
                        "    'table-name' = 'sum_money', " +
                        "    'username' = 'root', " +
                        "    'password' = '123456'," +
                        "    'sink.parallelism' = '1'" +
                        ")"
        );

        // 执行聚合查询，将结果写入 MySQL
        tableEnv.executeSql(
                "INSERT INTO mysql_sink " +
                        "SELECT " +
                        "    name, " +
                        "    SUM(money) AS total_money, " +
                        "    date_str " +
                        "FROM kafka_source " +
                        "GROUP BY name, date_str"
        );

    }
}
