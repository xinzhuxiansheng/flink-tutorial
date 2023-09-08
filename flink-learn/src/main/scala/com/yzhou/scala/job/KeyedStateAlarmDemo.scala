package com.yzhou.scala.job

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 温度告警，ValueState，当某个机房的相连温度值差 达到一定阈值，则触发报警
 */
object KeyedStateAlarmDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

    // 数据格式为 设备ID，温度
    val text = env.socketTextStream("yzhou.com", 7777)

    import org.apache.flink.api.scala._
    text.map(line => {
      val tup = line.split(",")
      (tup(0), tup(1).toInt)
    }).keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(String, Int), String] {
        // 声明一个ValueState类型的状态变量，存储设备上一次收到的温度数据
        private var lastDataState: ValueState[Int] = _

        // 任务初始化的时候，这个方法只执行一次
        override def open(parameters: Configuration): Unit = {
          // 注册状态
          val valueStateDesc = new ValueStateDescriptor[Int](
            "lastDataState", // 指定状态名称
            classOf[Int] // 指定状态中存储的数据类型
          )
          lastDataState = getRuntimeContext.getState(valueStateDesc)
        }

        override def flatMap(value: (String, Int), out: Collector[String]): Unit = {
          if (lastDataState.value() == null) {
            lastDataState.update(value._2)
          }
          // 获取上次温度
          val tmpLastData = lastDataState.value()
          // 如果某个设备的最近两次温差超过20度，则告警
          if (Math.abs(value._2 - tmpLastData) >= 20) {
            out.collect(value._1 + "_温度异常")
          }
          // 更新状态
          lastDataState.update(value._2)
        }
      })


  }
}
