package com.yzhou.job;

import com.yzhou.job.sink.MySQLSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ParallelismTest {
    private static Logger logger = LoggerFactory.getLogger(ParallelismTest.class);

    public static void main(String[] args) throws Exception {
        logger.info("******************* ParallelismTest job start *******************");

        // kafka server
        String kafkaServer = "k8s01:9092,k8s02:9092,k8s03:9092";
        // kafka topic
        String kafkaTopics = "first";
        // mysql数据库ip
        String dbHost = "k8s02";
        // mysql数据库端口
        String dbPort = "3306";
        // 数据库名称
        String dbName = "flink_test";
        // 结果表
        String tables = "wc";
        // checkpoint文件保存路径
        String checkpointPath = "file:////Users/a/TMP/flink_checkpoint/";
        // checkpoint保存时间间隔，默认10s
        long interval = 10000;
        // 并行度
        int parallelism = 1;

        // 从程序传参中获取参数
        if (args != null && args.length == 9) {
            kafkaServer = args[0];
            kafkaTopics = args[1];
            dbHost = args[2];
            dbPort = args[3];
            dbName = args[4];
            tables = args[5];
            checkpointPath = args[6];
            interval = Long.parseLong(args[7]);
            parallelism = Integer.parseInt(args[8]);

            logger.info("******************* kafkaServer=" + args[0] + ", " +
                    "kafkaTopic=" + args[1] + ", " +
                    "dbHost=" + args[2] + ", " +
                    "dbPort=" + args[3] + ", " +
                    "dbName=" + args[4] + ", " +
                    "table=" + args[5] + ", " +
                    "checkpointPath=" + args[6] + ", " +
                    "interval=" + args[7] + ", " +
                    "parallelism=" + args[8]);
        }

        // 0. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 1. 配置checkpoint
        env.enableCheckpointing(interval);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setCheckpointStorage(checkpointPath);

        int i = 0;
        for (String kafkaTopic : kafkaTopics.split(",")) {
            // 2. 配置Kafka
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", kafkaServer);
            properties.setProperty("group.id", "wc-consumer-group-"+i);
            properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty("auto.offset.reset", "latest");

            SingleOutputStreamOperator<String> stream = null;

            if (parallelism > 0) {
                // 3. 设置kafka source和算子并行度
                stream = env.addSource(new FlinkKafkaConsumer<String>(
                                kafkaTopic,
                                new SimpleStringSchema(),
                                properties
                        )).slotSharingGroup("slotGroup-" + i)   // 设置不同的source(topic)占用不同的slot，而不是共享slot
                        .name(kafkaTopic).setParallelism(parallelism);  // 设置source名字
            } else {
                // 3. 设置kafka source，但设置算子并行度
                stream = env.addSource(new FlinkKafkaConsumer<String>(
                                kafkaTopic,
                                new SimpleStringSchema(),
                                properties
                        )).slotSharingGroup("slotGroup-" + i)   // 设置不同的source(topic)占用不同的slot，而不是共享slot
                        .name(kafkaTopic);  // 设置source名字
            }

            logger.info("******************************* Source Parallelism = " + stream.getParallelism());

            // 4. 转换数据格式
            SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = stream
                    .flatMap((String line, Collector<String> words) -> {
                        Arrays.stream(line.split(" ")).forEach(words::collect);
                    })
                    .returns(Types.STRING)
                    .map(word -> Tuple2.of(word, 1L))
                    .returns(Types.TUPLE(Types.STRING, Types.LONG));

            // 5. 分组
            KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
                    .keyBy(t -> t.f0);

            // 6. 求和
            SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
                    .sum(1);

            String table = tables.split(",")[i];
            // 7. 设置自定义sink，结果输出到MySQL
            result.addSink(new MySQLSink(dbHost, dbPort, dbName, table)).name(table);

            i += 1;
        }
        env.execute("ParallelismTest");
    }
}
