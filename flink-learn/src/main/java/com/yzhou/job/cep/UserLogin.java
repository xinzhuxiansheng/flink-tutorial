//package com.yzhou.job.cep;
//
//import com.yzhou.job.functions.LoginWarning;
//import com.yzhou.job.pojo.LoginEvent;
//import lombok.val;
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class UserLogin {
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        DataStream<LoginEvent> loginEventDataStream = env.readTextFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/files/LoginLog.csv")
//                .map(data -> {
//                            // 将文件中的数据封装成样例类
//                            String[] dataArray = data.split(",");
//                            return new LoginEvent(dataArray[0], dataArray[1], dataArray[2], Long.parseLong(dataArray[3]));
//                        }
//                )
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<LoginEvent>forMonotonousTimestamps() // 单调递增策略
//                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
//                                    @Override
//                                    public long extractTimestamp(LoginEvent loginEvent, long l) {
//                                        return loginEvent.eventTime * 1000;
//                                    }
//                                }))      // 以用户id为key，进行分组
//                .keyBy(e->e.userId)
//                // 计算出同一个用户2秒内连续登录失败超过2次的报警信息
//                .process(new LoginWarning(2))
//                .print()
//
//
//
//
//    }
//}
