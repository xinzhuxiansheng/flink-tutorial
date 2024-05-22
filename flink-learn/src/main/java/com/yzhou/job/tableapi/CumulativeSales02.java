package com.yzhou.job.tableapi;

import com.yzhou.common.utils.FileUtil;
import org.apache.flink.table.api.*;

/*
    使用 SQL API 统计每种商品的累计销售额
 */
public class CumulativeSales02 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        String sourceSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/source/datagenSource01.sql");
        tEnv.executeSql(sourceSql);

        String sinksql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/sink/print02.sql");
        tEnv.executeSql(sinksql);

        String execSql = "INSERT INTO sink_table\n"
                + "SELECT\n"
                + " productId, sum(income) as `all`\n"
                + "FROM source_table\n"
                + "GROUP BY productId";
        tEnv.executeSql(execSql);
    }
}
