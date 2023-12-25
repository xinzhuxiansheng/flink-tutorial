package com.yzhou.scala.state

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 温度告警：ValueState
 */
object KeyedState_AlarmDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    // 数据格式为 设备ID，温度
    env.socketTextStream("yzhou.com",9001)


  }
}
