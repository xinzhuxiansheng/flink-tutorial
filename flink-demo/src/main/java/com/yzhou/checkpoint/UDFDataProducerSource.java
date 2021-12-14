package com.yzhou.checkpoint;

import com.alibaba.fastjson.JSONObject;
import com.yzhou.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * 自定义数据源 - SourceFunction
 *
 * @author yzhou
 * @date 2021/11/17
 */
public class UDFDataProducerSource {
    private static Logger logger = LoggerFactory.getLogger(UDFDataProducerSource.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String bootstrapServers = parameterTool.get("bootstrapServers");
        String topic = parameterTool.get("topic");


        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());
        dataStream.map(new RichMapFunction<SensorReading, String>() {
            KafkaProducer<String,String> producer;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                logger.info("构造 producer");
                producer = new KafkaProducer<String, String>(producerProperties(bootstrapServers));
            }

            @Override
            public String map(SensorReading sensorReading) throws Exception {
                producer.send(new ProducerRecord<>(topic, JSONObject.toJSONString(sensorReading)), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(null==metadata){
                            e.printStackTrace();
                        }
                    }
                });
                return null;
            }
        });
        env.execute("yzhou producer 自定义数据源");
    }

    // 实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading> {
        // 定义一个标志位，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //定义一个随机数发生器
            Random random = new Random();

            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 5; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                //控制输出频率
                Thread.sleep(5000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    //定义动态修改的参数
    private static Properties producerProperties(Object bootstrapServers) {
        logger.info("yzhou producerProperties");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("max.request.size", 10485760);
        properties.put("linger.ms", 100);
        properties.put("buffer.memory", "67108864");
        properties.put("compression.type", "snappy");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //SASL/SCRAM
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='adminpwd';");
        return properties;
    }
}
