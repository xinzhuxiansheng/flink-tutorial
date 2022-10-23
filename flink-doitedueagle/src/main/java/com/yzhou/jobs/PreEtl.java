package com.yzhou.jobs;

import com.yzhou.common.util.FlinkUtils;
import com.yzhou.pojo.DataBean;
import com.yzhou.udf.JsonToBeanFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PreEtl {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = FlinkUtils.createEnvironment(parameterTool);
        KafkaSource<String> kafkaSource =  FlinkUtils.createKafkaSource(parameterTool,SimpleStringSchema.class);
        DataStream<String> dataStream =  env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka source");

        // 解析数据
        SingleOutputStreamOperator<DataBean> beanStream = dataStream.process(new JsonToBeanFunc());

        beanStream.print();
        env.execute("dev test");
    }
}
