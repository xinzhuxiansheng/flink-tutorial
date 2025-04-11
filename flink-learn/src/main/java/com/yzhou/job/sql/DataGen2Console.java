package com.yzhou.job.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataGen2Console {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String db = "create database xm COMMENT '这里是注释信息'";
        tableEnv.executeSql(db);
        String ddl = "CREATE TABLE xm.t_user (\n"
                + "  id INT,\n"
                + "  name STRING,\n"
                + "  age INT,\n"
                + "  created_at DATE,\n"
                + "  updated_at TIMESTAMP(3) \n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',            -- 使用datagen作为连接器\n"
                + "  'fields.id.kind' = 'random',        -- id字段使用随机数据生成\n"
                + "  'fields.id.min' = '1',              -- id字段的最小值\n"
                + "  'fields.id.max' = '100',            -- id字段的最大值\n"
                + "  'fields.name.length' = '10',         -- name字段的长度\n"
                + "  'fields.age.min' = '18',         -- age字段的最小值\n"
                + "  'fields.age.max' = '60',         -- age字段的最大值\n"
                + "  'rows-per-second' = '3'            -- 每秒生成的行数\n"
                + ")";
        tableEnv.executeSql(ddl);
        //Table result = tableEnv.sqlQuery("select * from users");
        tableEnv.executeSql("select id,name,age from xm.t_user where id > 5 ").print();
        // 执行任务 show tables;
        env.execute("Flink SQL Demo");

    }
}
