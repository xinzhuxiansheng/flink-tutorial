package com.yzhou.jobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @author yzhou
 * @date 2022/10/24
 * <p>
 * nc -lk 8888
 * <p>
 * 使用布隆过滤器 过滤用户是否存在
 */
public class ActivityCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        /*
            u1,A1,view
            u1,A1,view
            u1,A1,view
            u1,A1,join
            u1,A1,join
            u2,A1,view
            u2,A1,join
            浏览次数： A1,view,1
            参与次数： A1,join,1
        */
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        // 按照活动ID，事件ID进行keyBy，同一个活动，同一个种事件的用户一定会进入同一个分区
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        });

        // 在同一组内进行聚合
        keyedStream.process(new KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>() {

            private transient ValueState<BloomFilter<String>> bloomFilterState;
            private transient ValueState<Integer> countState;
            private transient ValueState<Integer> uidCountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BloomFilter<String>> bloomFilterDescriptor = new ValueStateDescriptor<>("uid-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
                }));
                bloomFilterState = getRuntimeContext().getState(bloomFilterDescriptor);

                ValueStateDescriptor<Integer> uidCountStateDecriptor = new ValueStateDescriptor<Integer>("uid-count-state", Integer.class);
                countState = getRuntimeContext().getState(uidCountStateDecriptor);

                ValueStateDescriptor<Integer> countStateDescriptor = new ValueStateDescriptor<>("uid-count-state", Integer.class);
                uidCountState = getRuntimeContext().getState(countStateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, String> value, KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>.Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                String uid = value.f0;

                BloomFilter<String> bloomFilter = bloomFilterState.value();
                Integer uidCount = uidCountState.value();
                if (bloomFilter == null) {
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                    uidCount = 0;
                }
                // 这个用户在布隆过滤器中一定不存在就添加bloomFilter
                if (!bloomFilter.mightContain(uid)) {
                    bloomFilter.put(uid);
                    uidCount++;
                }
                // 更新状态
                bloomFilterState.update(bloomFilter);
                uidCountState.update(uidCount);

                Integer count = countState.value();
                if (count == null) {
                    count = 0;
                }
                count++;
                countState.update(count);

                // 输出
                out.collect(Tuple4.of(value.f1, value.f2, uidCount, count));
            }
        }).print();
        env.execute("ActivityCount");
    }
}
