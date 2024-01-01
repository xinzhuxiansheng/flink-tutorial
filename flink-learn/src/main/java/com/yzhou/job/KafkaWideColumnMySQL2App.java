package com.yzhou.job;

import com.alibaba.fastjson.JSON;
import com.yzhou.job.functions.MySQLAsyncFunction;
import com.yzhou.job.pojo.Access;
import com.yzhou.job.util.MySQLUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 与 MySQL 弄宽表, 基于 异步 I/O
 */
public class KafkaWideColumnMySQL2App {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("yzhou.com", 9527);
        int capacity = 20;
        SingleOutputStreamOperator<Tuple2<String, String>> result = AsyncDataStream.orderedWait(lines,
                new MySQLAsyncFunction(capacity),
                3000,
                TimeUnit.MILLISECONDS,
                capacity
        );
        result.print();
        env.execute("KafkaWideColumnMySQL2App");
    }
}
