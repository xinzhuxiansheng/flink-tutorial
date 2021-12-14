package com.yzhou.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 定义传感器数据样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object WindowsTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val stream = env.socketTextStream("127.0.0.1", 7777);

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //此方法是数据流的快捷方式，其中已知元素时间戳在每个并行流中单调递增。在这种情况下，系统可以通过跟踪上升时间戳自动且完美地生成水印
      //默认传进来的是升序时间
      //.assignAscendingTimestamps(_.timestamp*1000L)
      .assignTimestampsAndWatermarks(new MyAssigner())

    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) //开时间窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) //用reduce做增量聚合

    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    env.execute("window test")
  }

}


class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{

}