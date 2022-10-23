package com.yzhou.jobs;

import com.yzhou.common.util.FlinkUtils;
import com.yzhou.constant.EventID;
import com.yzhou.pojo.DataBean;
import com.yzhou.udf.JsonToBeanFunc;
import com.yzhou.udf.LocationFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author yzhou
 * @date 2022/10/23
 */
public class UserCount2 {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = FlinkUtils.createEnvironment(parameterTool);
        KafkaSource<String> kafkaSource = FlinkUtils.createKafkaSource(parameterTool, SimpleStringSchema.class);
        DataStream<String> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        // 解析数据
        SingleOutputStreamOperator<DataBean> beanStream = dataStream.process(new JsonToBeanFunc());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> bean.getEventId().equals(EventID.APP_LAUNCH));

        String url = parameterTool.getRequired("geo.http.url");
        String key = parameterTool.getRequired("geo.http.key");
        // 异步IO
        SingleOutputStreamOperator<DataBean> dataBeanWithLocation = AsyncDataStream.unorderedWait(
                filtered,
                new LocationFunction(url, key, 50), // 异步处理Http请求
                5,
                TimeUnit.SECONDS);

        // (province,1,1)
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> locationUserCount = dataBeanWithLocation.map(new MapFunction<DataBean, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(DataBean value) throws Exception {
                return Tuple3.of(value.getProvince(), value.getIsNew(), 1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2);

        locationUserCount.print();
        env.execute("UserCount");
    }
}
