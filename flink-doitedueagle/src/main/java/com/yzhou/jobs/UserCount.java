package com.yzhou.jobs;

import com.yzhou.common.util.FlinkUtils;
import com.yzhou.constant.EventID;
import com.yzhou.pojo.DataBean;
import com.yzhou.udf.JsonToBeanFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yzhou
 * @date 2022/10/23
 */
public class UserCount {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = FlinkUtils.createEnvironment(parameterTool);
        KafkaSource<String> kafkaSource = FlinkUtils.createKafkaSource(parameterTool, SimpleStringSchema.class);
        DataStream<String> dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        // 解析数据
        SingleOutputStreamOperator<DataBean> beanStream = dataStream.process(new JsonToBeanFunc());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> bean.getEventId().equals(EventID.APP_LAUNCH));
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> osNameIsNewAndOne = filtered
                .map(bean -> Tuple3.of(bean.getOsName(), bean.getIsNew(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        // 按照OS和IsNew KeyBy
        // (android,0,1)
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> res1 = osNameIsNewAndOne.keyBy(tp -> Tuple2.of(tp.f0, tp.f1),
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }))
                .sum(2);
        res1.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<Integer, Integer>>() {
            // 统计新老用户个数
            @Override
            public Tuple2<Integer, Integer> map(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        }).keyBy(t -> t.f0).sum(1).print("total");

        env.execute("UserCount");
    }
}
