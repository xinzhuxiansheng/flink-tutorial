package com.yzhou.common.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtils {

    public static StreamExecutionEnvironment createEnvironment(ParameterTool parameterTool){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String checkpointPath = parameterTool.getRequired("checkpoint.path");
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }

    // 泛型方法 支持自定义序列化
    public static <T>KafkaSource<T> createKafkaSource(ParameterTool parameterTool,Class<? extends DeserializationSchema<T>> deserializer) throws Exception {

        Properties properties =  parameterTool.getProperties();
        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));

        // 从Kafka中读取数据
        KafkaSourceBuilder<T> kafkaSourceBuilder  = KafkaSource.<T>builder()
                .setTopics(topics)
                .setValueOnlyDeserializer(deserializer.newInstance())
                .setProperties(properties);
         kafkaSourceBuilder.setProperty(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(),"false");
         return kafkaSourceBuilder.build();
    }

    public static void main(String[] args) throws IOException {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long checkpointInterval = parameterTool.getLong("checkpoint.interval", 30000L);
        String checkpointPath = parameterTool.getRequired("checkpoint.path");
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend(checkpointPath));

        List<String> topics = Arrays.asList(parameterTool.getRequired("kafka.input.topics").split(","));

        Properties properties =  parameterTool.getProperties();

        // 从Kafka中读取数据
        KafkaSource<String> kafkaConsumer = KafkaSource.<String>builder()
                .setTopics(topics)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties).build();

        env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(),"kafka source");

    }
}
