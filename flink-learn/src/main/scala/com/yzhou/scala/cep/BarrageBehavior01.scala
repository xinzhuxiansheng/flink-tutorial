package com.yzhou.scala.cep

import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 规则：用户如果在 10s 内，同时输入 TMD 超过 5 次，就认为用户为恶意攻击，识别出该用户
 */
object BarrageBehavior01 {
  case class  LoginEvent(userId:String, message:String, timestamp:Long){
    override def toString: String = userId
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用IngestionTime作为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 用于观察测试数据处理顺序
    env.setParallelism(1)

    // 模拟数据源
    val loginEventStream: DataStream[LoginEvent] = env.fromCollection(
      List(
        LoginEvent("1", "TMD", 1618498576),
        LoginEvent("1", "TMD", 1618498577),
        LoginEvent("1", "TMD", 1618498579),
        LoginEvent("1", "TMD", 1618498582),
        LoginEvent("2", "TMD", 1618498583),
        LoginEvent("1", "TMD", 1618498585)
      )
    ).assignAscendingTimestamps(_.timestamp * 1000)

    //定义模式
    val loginEventPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.message == "TMD")
      .times(5)
      .within(Time.seconds(10))

    //匹配模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginEventPattern)

    import scala.collection.Map
    val result = patternStream.select((pattern:Map[String, Iterable[LoginEvent]])=> {
      val first = pattern.getOrElse("begin", null).iterator.next()
      (first.userId, first.timestamp)
    })
    //恶意用户，实际处理可将按用户进行禁言等处理，为简化此处仅打印出该用户
    result.print("恶意用户>>>")
    env.execute("BarrageBehavior01")
  }
}
