package com.yzhou.perf;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerPerfTestJob {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerPerfTestJob.class);

    public static void main(String[] args) throws Exception {
        //注意包名 java/scala
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        logger.info("========================running job without checkpoint===========================");
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(1, TimeUnit.MINUTES), Time.of(10, TimeUnit.MINUTES)));

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String bootstrapServers = parameterTool.get("bootstrapServers");
        String topic = parameterTool.get("topic");
        String singleTaskQPS = parameterTool.get("singleTaskQPS");
        String recordSize = parameterTool.get("recordSize");


        Map<String, Object> kafkaConf = new HashMap<>();
        //kafkaConf.put("bootstrap.servers", bootstrapServers);
        kafkaConf.put("bootstrap.servers", "10.24.15.123:9092");
        kafkaConf.put("acks", "1");
        kafkaConf.put("batch.size", 1024000); //1MB
        kafkaConf.put("compression.type", "snappy");
        kafkaConf.put("linger.ms", 200);

        Map<String,Object> sourceConf = new HashMap<>();
        sourceConf.put("singleTaskQPS",10000);
        sourceConf.put("recordSize",1024);

        DataStreamSource<String> source = env.addSource(new SourceTest(sourceConf));
        DataStream<String> filterSSData = source.map(new RichMapFunction<String, String>() {

            KafkaProducer<String, String> producer;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                producer = new KafkaProducer<String, String>(producerProperties(
                        kafkaConf.get("bootstrap.servers"),
                        kafkaConf.get("batch.size"),
                        kafkaConf.get("compression.type"),
                        kafkaConf.get("linger.ms")));
            }

            @Override
            public String map(String s) throws Exception {

                producer.send(new ProducerRecord<>("yzhoutp02", s), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (null == recordMetadata) {
                            e.printStackTrace();
                        }
                    }
                });
                return null;
            }
        });

        env.execute("kafka producer perf test job");
    }

    //定义动态修改的参数
    private static Properties producerProperties(Object bootstrapServers, Object batchSize, Object compressionType, Object lingerMs) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("max.request.size", 10485760);
        properties.put("batch.size", batchSize);
        properties.put("linger.ms", lingerMs);
        properties.put("buffer.memory", "67108864");
        properties.put("compression.type", compressionType);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //SASL/SCRAM
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='adminpwd';");
        return properties;
    }
}
