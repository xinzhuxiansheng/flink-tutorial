package com.yzhou.flink.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 入门案例二，使用 ParameterTool 工具类 接受 main() 参数
 */
public class GettingStartedCase02 {
    private static final Logger logger = LoggerFactory.getLogger(GettingStartedCase02.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        logger.info("receive main() params： ");
        // 解析命令行参数
        final ParameterTool params = ParameterTool.fromArgs(args);
        String arg01 = params.get("arg01", "defaultArg01");
        logger.info("GettingStartedCase02 print params: arg01 {}",arg01);

        // 配置checkpoint
        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.enableUnalignedCheckpoints();
        // checkpointConfig.setCheckpointStorage();

        // 创建 Kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("dn-kafka3:9092")
                .setTopics("yzhoujsontp01")
                .setGroupId("ygid02021")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        DataStreamSource<String> kfkStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");

        kfkStream.print();
        env.execute("flink jar01");
    }
}
