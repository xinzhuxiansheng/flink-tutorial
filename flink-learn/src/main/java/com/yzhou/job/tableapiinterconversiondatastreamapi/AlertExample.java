package com.yzhou.job.tableapiinterconversiondatastreamapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/*
    CUMULATE 函数：
        CUMULATE 是 Flink SQL 中用于创建累积窗口的函数。它会生成一系列固定长度的窗口，这些窗口随着时间的推移而逐渐增长。
        在这个查询中，CUMULATE 的参数定义了：
            源表：source_table，这是要处理的数据表。
            时间属性：DESCRIPTOR(row_time)，这指定了表中用于时间窗口计算的时间属性字段。
            步长：INTERVAL '5' SECOND，这是窗口增长的步长，意味着每5秒钟窗口会增长一次。
            最大长度：INTERVAL '1' DAY，这是窗口的最大长度，即窗口在达到一天长时停止增长。

    选择和聚合操作：
        SELECT 子句指定了查询的输出字段：
            window_end：使用 UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 将窗口结束时间转换为 UNIX 时间戳格式（毫秒为单位）。
            window_start：窗口的开始时间。
            sum(money)：计算每个窗口内 money 字段的总和，命名为 sum_money。
            count(distinct id)：计算每个窗口内不同 id 的数量，命名为 count_distinct_id。

    GROUP BY 子句：
        查询结果按照 window_start 和 window_end 进行分组，这意味着每个组代表一个不同的窗口时间范围。
 */
@Slf4j
public class AlertExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createTableSql = "CREATE TABLE source_table (\n"
                + "    id BIGINT,\n"
                + "    money BIGINT,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp_LTZ(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '1',\n"
                + "  'fields.money.max' = '100000'\n"
                + ")\n";

        String querySql = "SELECT UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n"
                + "      window_start, \n"
                + "      sum(money) as sum_money,\n"
                + "      count(distinct id) as count_distinct_id\n"
                + "FROM TABLE(CUMULATE(\n"
                + "         TABLE source_table\n"
                + "         , DESCRIPTOR(row_time)\n"
                + "         , INTERVAL '5' SECOND\n"
                + "         , INTERVAL '1' DAY))\n"
                + "GROUP BY window_start, \n"
                + "        window_end";

        // 1. 创建数据源表，即优惠券发放明细数据
        tableEnv.executeSql(createTableSql);
        // 2. 执行 query 查询，计算每日发放金额
        Table resultTable = tableEnv.sqlQuery(querySql);

        // 3. 报警逻辑（toDataStream 返回 Row 类型），如果 sum_money 超过 1w，报警
        tableEnv
                .toDataStream(resultTable, Row.class)
                .flatMap(new FlatMapFunction<Row, Object>() {
                    @Override
                    public void flatMap(Row value, Collector<Object> out) throws Exception {
                        long l = Long.parseLong(String.valueOf(value.getField("sum_money")));

                        if (l > 10000L) {
                            log.info("报警，超过 1w");
                        }
                    }
                });
        env.execute();
    }
}

