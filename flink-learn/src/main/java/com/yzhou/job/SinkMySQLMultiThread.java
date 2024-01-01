package com.yzhou.job;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.yzhou.job.pojo.Student;
import com.yzhou.job.sink.SinkToMySQLMultiThread;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * refer: https://blog.csdn.net/qq_23160237/article/details/103821970
 */
public class SinkMySQLMultiThread {
    public static void main(String[] args) throws Exception{
        //1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/a/TMP/flink_checkpoint");

        env.setParallelism(1);

        String brokers = "kafka:9092";
        String topic = "tp01";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId("yzhougid202401")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KAFKA-SOURCE")
                .map(str -> new Gson().fromJson(str, Student.class))
                .addSink(new SinkToMySQLMultiThread());
        env.execute();
    }
}
