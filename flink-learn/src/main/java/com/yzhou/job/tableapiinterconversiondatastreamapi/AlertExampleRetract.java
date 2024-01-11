package com.yzhou.job.tableapiinterconversiondatastreamapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/*
    Retract 流 转 DataStream
 */
@Slf4j
public class AlertExampleRetract {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String createTableSql = "CREATE TABLE source_table (\n"
                + "    id BIGINT,\n"
                + "    money BIGINT,\n"
                + "    `time` as cast(UNIX_TIMESTAMP() as bigint) * 1000\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '1',\n"
                + "  'fields.money.max' = '100000'\n"
                + ")\n";

        String querySql = "SELECT max(`time`), \n"
                + "      sum(money) as sum_money\n"
                + "FROM source_table\n"
                + "GROUP BY (`time` + 8 * 3600 * 1000) / (24 * 3600 * 1000)";

        tableEnv.executeSql(createTableSql);

        Table resultTable = tableEnv.sqlQuery(querySql);

        tableEnv
                .toRetractStream(resultTable, Row.class)
                .flatMap(new FlatMapFunction<Tuple2<Boolean, Row>, Object>() {
                    @Override
                    public void flatMap(Tuple2<Boolean, Row> value, Collector<Object> out) throws Exception {
                        long l = Long.parseLong(String.valueOf(value.f1.getField("sum_money")));

                        if (l > 10000L) {
                            log.info("报警，超过 1w");
                        }
                    }
                });

        env.execute();
    }

}
