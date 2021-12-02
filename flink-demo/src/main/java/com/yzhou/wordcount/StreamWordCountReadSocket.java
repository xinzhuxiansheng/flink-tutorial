package com.yzhou.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCountReadSocket {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        //env.setParallelism(8);

        //用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //需要在终端，执行 nc -lk 7777
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        //基于数据流进行转换操作
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new DataSetWordCountReadFile.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();
        //execute针对流处理mode
        env.execute();
    }
}
