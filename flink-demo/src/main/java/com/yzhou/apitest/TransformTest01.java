package com.yzhou.apitest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yzhou
 * @date 2021/12/13
 */
public class TransformTest01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream =  env.readTextFile("/Users/yiche/Code/JAVA/yzhou/flink-tutorial/flink-demo/src/main/resources/hello.txt");

        //1. map 把String转换成长度输出
        DataStream<Integer> mapStream =  inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        //2. flatmap 按逗号切分字段
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for(String field:fields){
                    collector.collect(field);
                }
            }
        });
    }
}
