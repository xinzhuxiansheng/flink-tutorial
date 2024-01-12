package com.yzhou.job.tableapi;

import org.apache.flink.table.api.*;

/*
    使用 Table API 统计每种商品的累计销售额
 */
public class CumulativeSales {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 从数据源存储引擎中读取数据，返回值为 Table
        Table sourceTable =  tEnv.from(TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("productId", DataTypes.BIGINT())
                        .column("income",DataTypes.BIGINT())
                .build())
                .option("rows-per-second","1")
                .option("fields.productId.min","1")
                .option("fields.productId.max","2")
                .option("fields.income.min","1")
                .option("fields.income.max","2")
                .build());

        Table transformTable = sourceTable
                .groupBy(Expressions.$("productId"))
                .select(Expressions.$("productId"), Expressions.$("income").sum()).as("all");

        // 将结果写到数据存储引擎中
        TableResult tableResult = transformTable.executeInsert(TableDescriptor.forConnector("print")
                .schema(Schema.newBuilder()
                        .column("productId", DataTypes.BIGINT())
                        .column("all", DataTypes.BIGINT())
                        .build())
                .build());

    }
}
