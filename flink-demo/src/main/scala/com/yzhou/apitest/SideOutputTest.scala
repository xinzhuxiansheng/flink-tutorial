package com.yzhou.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutPutTest {
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

    val processedStream = dataStream
      .process(new FreezingAlert())

    processedStream.print("processed data")
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

    env.execute("window test")
  }

}

// 冰点报警，如果小于32F，输出报警信息到侧输出流
class FreezingAlert extends ProcessFunction[SensorReading, SensorReading] {

  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      ctx.output(alertOutput, "freezing alert for " + value.id)
    } else {
      out.collect(value)
    }
  }
}
