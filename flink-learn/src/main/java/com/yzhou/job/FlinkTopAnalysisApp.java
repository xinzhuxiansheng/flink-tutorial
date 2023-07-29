package com.yzhou.job;

import com.alibaba.fastjson.JSON;
import com.yzhou.common.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTopAnalysisApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // step1: 接入要处理的数据源
        DataStreamSource<String> source = env.readTextFile("data/productaccess.log");

        // step2: 使用Flink Transformation算子进行各种维度的统计分析
        /**
         * 接入的数据是json格式 ==> Access
         */
        SingleOutputStreamOperator<Tuple3<String, String, Long>> resultStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                return JSON.parseObject(value, Access.class);
            }
        }).map(new MapFunction<Access, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(Access x) throws Exception {
                return Tuple3.of(x.getName(), DateUtils.ts2Date(x.getTs(), "yyyyMMdd"), 1L);
            }
        }).keyBy(new KeySelector<Tuple3<String, String, Long>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Long> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2)
                .map(new MapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(Tuple3<String, String, Long> value) throws Exception {
                        return Tuple3.of("pk-access-"+value.f1, value.f0, value.f2);
                    }
                });

        //resultStream.addSink(new PKRedisSink());
        resultStream.print();

        env.execute("FlinkTopAnalysisApp");
    }

}
