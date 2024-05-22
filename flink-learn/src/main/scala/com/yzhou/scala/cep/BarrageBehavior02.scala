package com.yzhou.scala.cep

import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
object BarrageBehavior02 {
  case class Message(userId: String, ip: String, msg: String)

  /**
   * 规则：用户如果在 10s 内，同时连续输入同样一句话超过 5 次，就认为是恶意刷屏。
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //初始化运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    // 模拟数据源
    val loginEventStream: DataStream[Message] = env.fromCollection(
      List(
        Message("1", "192.168.0.1", "beijing"),
        Message("1", "192.168.0.2", "beijing"),
        Message("1", "192.168.0.3", "beijing"),
        Message("1", "192.168.0.4", "beijing"),
        Message("2", "192.168.10.10", "shanghai"),
        Message("3", "192.168.10.10", "beijing"),
        Message("3", "192.168.10.11", "beijing"),
        Message("4", "192.168.10.10", "beijing"),
        Message("5", "192.168.10.11", "shanghai"),
        Message("4", "192.168.10.12", "beijing"),
        Message("5", "192.168.10.13", "shanghai"),
        Message("5", "192.168.10.14", "shanghai"),
        Message("5", "192.168.10.15", "beijing"),
        Message("6", "192.168.10.16", "beijing"),
        Message("6", "192.168.10.17", "beijing"),
        Message("6", "192.168.10.18", "beijing"),
        Message("5", "192.168.10.18", "shanghai"),
        Message("6", "192.168.10.19", "beijing"),
        Message("6", "192.168.10.19", "beijing"),
        Message("5", "192.168.10.18", "shanghai")
      )
    )

    //定义模式
    val loginbeijingPattern = Pattern.begin[Message]("start")
      .where(_.msg != null) //一条登录失败
      .times(5).optional  //将满足五次的数据配对打印
      .within(Time.seconds(10))

    //进行分组匹配
    val loginbeijingDataPattern = CEP.pattern(loginEventStream.keyBy(_.userId), loginbeijingPattern)

    //查找符合规则的数据
    val loginbeijingResult: DataStream[Option[Iterable[Message]]] = loginbeijingDataPattern.select(patternSelectFun = (pattern: collection.Map[String, Iterable[Message]]) => {
      var loginEventList: Option[Iterable[Message]] = null
      loginEventList = pattern.get("start") match {
        case Some(value) => {
          if (value.toList.map(x => (x.userId, x.msg)).distinct.size == 1) {
            Some(value)
          } else {
            None
          }
        }
      }
      loginEventList
    })

    //打印测试
    loginbeijingResult.filter(x=>x!=None).map(x=>{
      x match {
        case Some(value)=> value
      }
    }).print()

    env.execute("BarrageBehavior02")
  }
}
