package com.yzhou.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        //env.setParallelism(8);
        //从文件中读取数据
        String inputPath = "/Users/yiche/Code/JAVA/yzhou/flink-tutorial/flink-java/src/main/resources/hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);
        //基于数据流进行转换操作
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new DataSetWordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();
        //execute针对流处理mode
        env.execute();

//        1> (scala,1)
//        7> (flink,1)
//        5> (world,1)
//        3> (hello,1)
//        1> (spark,1)
//        3> (hello,2)
//        3> (hello,3)
//        3> (hello,4)
//        5> (world,2)
//        3> (hello,5)

    }
}
