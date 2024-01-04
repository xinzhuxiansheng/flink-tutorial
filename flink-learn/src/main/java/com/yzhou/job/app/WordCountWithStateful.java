package com.yzhou.job.app;

import com.yzhou.job.functions.WordCountStateFunction;
import com.yzhou.job.pojo.WordCount;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountWithStateful {
    public static void main(String[] args) throws Exception {

        // 加载上下文环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Source 加载数据
        DataStream<String> lines =
                env.socketTextStream("127.0.0.1",8888,"\n");

        lines.flatMap((String input, Collector<WordCount> output)-> {
                            //第1步：切割 和 标记
                            String[] words = input.split(" ");
                            for(String word:words) {
                                output.collect(new WordCount(word,1));
                            }
                        }
                ).returns(WordCount.class)
                //第2步 分组
                .keyBy(WordCount::getWord)
                //第3步 状态计算
                .flatMap(new WordCountStateFunction())
                //打印
                .print();
        // Sink 数据输出
        System.out.println("####### 基于状态(KeyedState)计算实现词频统计 ###########");
        // DataStream 要执行 execute 方法才会触发程序运行
        env.execute();

    }
}
