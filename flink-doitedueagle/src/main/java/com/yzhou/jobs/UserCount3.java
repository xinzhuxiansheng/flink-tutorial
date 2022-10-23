package com.yzhou.jobs;

import com.yzhou.common.util.FlinkUtils;
import com.yzhou.constant.EventID;
import com.yzhou.pojo.DataBean;
import com.yzhou.udf.IsNewUserFunction;
import com.yzhou.udf.JsonToBeanFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yzhou
 * @date 2022/10/23
 */
public class UserCount3 {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = FlinkUtils.createEnvironment(parameterTool);
        KafkaSource<String> kafkaSource = FlinkUtils.createKafkaSource(parameterTool, SimpleStringSchema.class);
        DataStream<String> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        // 解析数据
        SingleOutputStreamOperator<DataBean> beanStream = dataStream.process(new JsonToBeanFunc());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> bean.getEventId().equals(EventID.APP_LAUNCH));

        // 先计算设备ID是不是一个新用户
        // 按照手机型号进行KeyBy
        KeyedStream<DataBean, String> keyed = filtered.keyBy(DataBean::getDeviceType);
        keyed.process(new IsNewUserFunction()).print();
        env.execute("UserCount3");
    }
}
