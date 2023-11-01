package com.yzhou.job.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
 * >>>>> 练习题需求 >>>>>>>
 *     基本： kafka中有如下数据：
 *         {"id":1,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
 *
 *     高级：kafka中有如下数据：
 *      {"id":1,"name":{"formal":"zs","nick":"tiedan"},"age":18,"gender":"male"}
 *
 *      现在需要用flinkSql来对上述数据进行查询统计：
 *        截止到当前,每个昵称,都有多少个用户
 *        截止到当前,每个性别,年龄最大值
 */
public class Demo6_Exercise {
    public static void main(String[] args) {
        StreamExecutionEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        TableEnvironment tenv = StreamTableEnvironment.create(localEnvironment,EnvironmentSettings.inStreamingMode());

        // 建表（数据源表）
        tenv.executeSql(
                "CREATE TABLE `yzhou_source_yzhou_test01`\n" +
                        "(\n" +
                        "    `id`      INT    NOT NULL COMMENT '',\n" +
                        "    `name`    STRING NOT NULL COMMENT '',\n" +
                        "    `address` STRING COMMENT '',\n" +
                        "    PRIMARY KEY (id) NOT ENFORCED\n" +
                        ")\n" +
                        "    WITH\n" +
                        "        (\n" +
                        "        'connector' = 'mysql-cdc',\n" +
                        "        'hostname' = '127.0.0.1',\n" +
                        "        'port' = '3306',\n" +
                        "        'username' = 'root',\n" +
                        "        'password' = '12345678',\n" +
                        "        'database-name' = 'yzhou_test',\n" +
                        "        'table-name' = 'source_yzhou_test01',\n" +
                        "        'server-id' = '5401',\n" +
                        "        'scan.startup.mode' = 'initial',\n" +
                        "        'scan.incremental.snapshot.chunk.key-column' = 'id',\n" +
                        "        'scan.incremental.snapshot.chunk.size' = '8096'\n" +
                        "        )"
        );


        tenv.executeSql(
                "CREATE TABLE `yzhou_source_yzhou_test02`\n" +
                        "(\n" +
                        "    `id`      INT    NOT NULL COMMENT '',\n" +
                        "    `name`    STRING NOT NULL COMMENT '',\n" +
                        "    `address` STRING COMMENT '',\n" +
                        "    PRIMARY KEY (id) NOT ENFORCED\n" +
                        ")\n" +
                        "    WITH\n" +
                        "        (\n" +
                        "        'connector' = 'mysql-cdc',\n" +
                        "        'hostname' = '127.0.0.1',\n" +
                        "        'port' = '3306',\n" +
                        "        'username' = 'root',\n" +
                        "        'password' = '12345678',\n" +
                        "        'database-name' = 'yzhou_test',\n" +
                        "        'table-name' = 'source_yzhou_test02',\n" +
                        "        'server-id' = '5402',\n" +
                        "        'scan.startup.mode' = 'initial',\n" +
                        "        'scan.incremental.snapshot.chunk.key-column' = 'id',\n" +
                        "        'scan.incremental.snapshot.chunk.size' = '8096'\n" +
                        "        )"
        );


        tenv.executeSql(
            "CREATE TABLE sink_yzhou_test01 (\n" +
                    "    id INT PRIMARY KEY NOT ENFORCED,\n" +
                    "    name VARCHAR(50) NOT NULL,\n" +
                    "    address VARCHAR(100),\n" +
                    "    ext_field01 VARCHAR(100)\n" +
                    ") WITH (\n" +
                    "    'connector' = 'jdbc',\n" +
                    "    'url' = 'jdbc:mysql://localhost:3306/yzhou_test',\n" +
                    "    'table-name' = 'sink_yzhou_test01',\n" +
                    "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                    "    'username' = 'root',\n" +
                    "    'password' = '12345678'\n" +
                    ")"
        );

        tenv.executeSql(
                "CREATE TABLE sink_yzhou_test02 (\n" +
                        "    id INT PRIMARY KEY NOT ENFORCED,\n" +
                        "    name VARCHAR(50) NOT NULL,\n" +
                        "    address VARCHAR(100),\n" +
                        "    ext_field01 VARCHAR(100)\n" +
                        ") WITH (\n" +
                        "    'connector' = 'jdbc',\n" +
                        "    'url' = 'jdbc:mysql://localhost:3306/yzhou_test',\n" +
                        "    'table-name' = 'sink_yzhou_test02',\n" +
                        "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        "    'username' = 'root',\n" +
                        "    'password' = '12345678'\n" +
                        ")"
        );


        // 查询 并 打印
        //TableResult tableResult = tenv.executeSql("select nick,count(distinct id) as user_cnt from t_person group by nick");
        tenv.executeSql(
                "INSERT INTO sink_yzhou_test01\n" +
                        "SELECT *\n" +
                        "FROM yzhou_source_yzhou_test01 \n" +
                        "INSERT INTO sink_yzhou_test02\n" +
                        "SELECT *\n" +
                        "FROM yzhou_source_yzhou_test02;"
                );

    }
}
