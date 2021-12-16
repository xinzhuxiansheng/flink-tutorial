package com.yzhou.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

// 定义传感器数据样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object WindowsTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置Watermarker间隔时间
    //    env.getConfig.setAutoWatermarkInterval(100L)


    val stream = env.socketTextStream("127.0.0.1", 8888);

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //此方法是数据流的快捷方式，其中已知元素时间戳在每个并行流中单调递增。在这种情况下，系统可以通过跟踪上升时间戳自动且完美地生成水印
      //默认传进来的是升序时间
      //.assignAscendingTimestamps(_.timestamp*1000L)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //Time.hours(-8) 设置时区
//      .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5)
//        , Time.hours(-8)))
      //滚动窗口 | 滑动窗口 包括步长
      .timeWindow(Time.seconds(15), Time.seconds(5)) //开时间窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) //用reduce做增量聚合



    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    env.execute("window test")
  }

}


//class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
//  val bound = 1000 * 60
//  var maxTs = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = new Watermark(1)
//
//  override def extractTimestamp(t: SensorReading, l: Long): Long = {
//    maxTs = maxTs.max(t.timestamp * 1000)
//    t.timestamp * 1000
//  }
//}

class MyAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = new Watermark(l);

  override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp * 1000
}