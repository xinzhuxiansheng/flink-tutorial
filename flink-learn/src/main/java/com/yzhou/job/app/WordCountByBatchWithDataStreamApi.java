package com.yzhou.job.app;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 通过 DataStream API ，以批处理的方式进行词频统计
 */
public class WordCountByBatchWithDataStreamApi {
    private static Logger logger = Logger.getLogger(WordCountByBatchWithDataStreamApi.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 2. Source 加载数据
        DataStream<String> lines = env.fromElements(
                "imooc hadoop flink",
                "imooc hadoop flink",
                "imooc hadoop",
                "imooc"
        );
        // 3. 转换数据格式
        // 将每个 word 输出成 Tuple2.of(word,1)
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lines
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
                .keyBy(t -> t.f0);
        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
                .sum(1);
        // 6. 打印
        result.print();
        logger.info(result.toString());
        // 7. 执行
        env.execute();
    }
}
