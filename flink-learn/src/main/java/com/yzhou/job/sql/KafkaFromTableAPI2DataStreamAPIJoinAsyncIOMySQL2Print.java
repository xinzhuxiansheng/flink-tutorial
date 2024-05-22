package com.yzhou.job.sql;

import com.yzhou.common.utils.FileUtil;
import com.yzhou.job.functions.MySQLAsyncFunctionSupportRowType;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class KafkaFromTableAPI2DataStreamAPIJoinAsyncIOMySQL2Print {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // env.enableCheckpointing(5000);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册表
        // kafka
        String createSourceTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/source/kafkajson.sql");
        TableResult sourceTableResult = tableEnv.executeSql(createSourceTableSql);
        // mysql
        String createSinkTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/sink/print01.sql");;
        TableResult sinkTableResult = tableEnv.executeSql(createSinkTableSql);

        // 维表 SQL
        /*
            create table address
            (
                id       int auto_increment
                    primary key,
                province varchar(100) null,
                city     varchar(100) null
            );
         */

        // 转 Table
        String querySourceTableSql = "select * from kafka_source";
        Table sourceTable = tableEnv.sqlQuery(querySourceTableSql);

        // 创建 kafka 临时表
        // tableEnv.createTemporaryView("kafka", kafkaTable);
        DataStream<Row> dataStream = tableEnv.toDataStream(sourceTable);

        SingleOutputStreamOperator<Row> result = AsyncDataStream.orderedWait(dataStream,
                new MySQLAsyncFunctionSupportRowType(20),
                30000,
                TimeUnit.MILLISECONDS,
                20);

//        StreamStatementSet statementSet = tableEnv.createStatementSet();
//        statementSet.addInsert("mysql_sink",sourceTable);

        result.print();

        // 执行
        //statementSet.execute();
        env.execute();
    }
}
