package com.yzhou.job;

import com.alibaba.fastjson.JSON;
import com.yzhou.job.pojo.Access;
import com.yzhou.job.util.MySQLUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 与 MySQL 弄宽表 Map付函数
 */
public class KafkaWideColumnMySQLApp {
    public static void main(String[] args) throws Exception {

        // localhost:8081
        Configuration configuration = new Configuration();
        // configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        String brokers = "kafka:9092";
        String topic = "tp01";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId("yzhougid202401")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 已经对接上Kafka
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "PK-KAFKA-SOURCE");

        // 假设只有id，需要根据id讲mysql中的名称补齐
        source.map(json -> JSON.parseObject(json, Access.class))
                .map(new RichMapFunction<Access, Access>() {
                    Connection connection = null;
                    PreparedStatement pstmt = null;
                    Map<Integer, String> courseMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = MySQLUtils.getConnection();
                        pstmt = connection.prepareStatement("select * from product");
                        ResultSet rs = pstmt.executeQuery();
                        while(rs.next()) {
                            courseMap.put(rs.getInt("id"), rs.getString("name"));
                        }
                    }

                    @Override
                    public Access map(Access value) throws Exception {
                        value.setName(courseMap.get(value.getId()));
                        return value;
                    }

                    @Override
                    public void close() throws Exception {
                        MySQLUtils.close(pstmt);
                        MySQLUtils.close(connection);
                    }
                }).print();

        env.execute("KafkaWideColumnMySQLApp");
    }
}
