package com.yzhou.perf;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerPerfTestJob {
    private final static Logger logger = LoggerFactory.getLogger(KafkaConsumerPerfTestJob.class);

    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Map<String, Object> params = new HashMap<>();


//        Options options = new Options();
//        options.addOption("topic", true, "Topic Name")
//                .addOption("groupIdCount", true, "groupIdCount")
//                .addOption("groupIdPrefix", true, "groupIdPrefix")
//                .addOption("kafkaConsumerNum", true, "kafkaConsumerNum");
//
//        HelpFormatter formatter = new HelpFormatter();
//        formatter.printHelp("TestJob", options);
//
//        CommandLineParser parser = new DefaultParser();
//        CommandLine cmd = parser.parse(options, args);
//
//        String topic = cmd.getOptionValue("topic");
//        String token = cmd.getOptionValue("token", "dc");
//        String groupIdCount = cmd.getOptionValue("groupIdCount");
//        String groupIdPrefix = cmd.getOptionValue("groupIdPrefix");
//        final int kafkaConsumerNum = Integer.parseInt(cmd.getOptionValue("kafkaConsumerNum"));

        params.put("topic", "yzhoutp01");
        params.put("groupIdCount", 1);
        params.put("groupIdPrefix", "perfgid");
        params.put("kafkaConsumerNum", 1);
        params.put("bootstrap.servers", "192.168.70.34:9092");


        for (int i = 1; i <= (int) params.get("kafkaConsumerNum"); i++) {
            FlinkKafkaConsumer<String> consumer = createConsumer(
                    params.get("bootstrap.servers").toString(),
                    params.get("topic").toString(),
                    params.get("groupIdPrefix") + "-" + i);
            consumer.setStartFromLatest();
            DataStreamSource<String> sourceStream = env.addSource(consumer);
            sourceStream.addSink(S.getInstance());
        }

        env.execute("818 consumer Test job");
    }

    static class S extends RichSinkFunction<String> {
        long ts;

        private S() {

        }

        static S getInstance() {
            return new S();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ts = System.currentTimeMillis();
            super.open(parameters);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis > ts + 1000) {
                Thread.sleep(10);
                logger.info("??????");
                ts = currentTimeMillis;
            }
        }
    }

    public static FlinkKafkaConsumer createConsumer(String bootstrapServers, String topicName, String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        //properties.put("auto.offset.reset", "earliest");
        properties.put("auto.commit.interval.ms", "1000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='yzhou' password='yzhoupwd';");
        properties.put("flink.partition-discovery.interval-millis", 1000 * 30 * 60);
        properties.put("max.partition.fetch.bytes", "20971520");

        return new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), properties);
    }
}
