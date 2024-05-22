package com.yzhou.scala.state

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * 实时计算事件总个数，以及value总和
 */
object EventCounterJob {

  def main(args: Array[String]): Unit = {
    var configuration =  new Configuration()
    configuration.setString("execution.savepoint.path","file:///Users/a/TMP/flink_checkpoint/8781988ef03014886759192ebd228383/chk-69");
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)

    // 配置checkpoint
    // 设置状态后端为HashMapStateBackend
    env.setStateBackend(new HashMapStateBackend());
    // 做两个checkpoint的间隔为1秒
    env.enableCheckpointing(1000)
    // 表示下 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint 会在整个作业 Cancel 时被删除。Checkpoint 是作业级别的保存点。
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointStorage("file:///Users/a/TMP/flink_checkpoint")

    // 1. 从socket中接收文本数据
    val streamText: DataStream[String] = env.socketTextStream("127.0.0.1", 9000)

    // 2. 将文本内容按照空格分割转换为事件样例类
    val events = streamText.map(s => {
      val tokens = s.split(" ")
      Event(tokens(0), tokens(1).toDouble, tokens(2).toLong)
    })
    // 3. 按照时间id分区，然后进行聚合统计
    val counterResult = events.keyBy(_.id).process(new EventCounterProcessFunction)

    // 4. 结果输出到控制台
    counterResult.print()

    env.execute("EventCounterJob")
  }
}

/**
 * 定义事件样例类
 *
 * @param id    事件类型id
 * @param value 事件值
 * @param time  事件时间
 */
case class Event(id: String, value: Double, time: Long)

/**
 * 定义事件统计器样例类
 *
 * @param id    事件类型id
 * @param sum   事件值总和
 * @param count 事件个数
 */
case class EventCounter(id: String, var sum: Double, var count: Int)

/**
 * 继承KeyedProcessFunction实现事件统计
 */
class EventCounterProcessFunction extends KeyedProcessFunction[String, Event, EventCounter] {
  private var counterState: ValueState[EventCounter] = _
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 从flink上下文中获取状态, getRuntimeContext 示例类型是 StreamingRuntimeContext.
    counterState = getRuntimeContext.getState(new ValueStateDescriptor[EventCounter]("event-counter", classOf[EventCounter]))
    println("state 初始化完毕")
  }

  override def processElement(i: Event,
                              context: KeyedProcessFunction[String, Event, EventCounter]#Context,
                              collector: Collector[EventCounter]): Unit = {
    // 从状态中获取统计器，如果统计器不存在给定一个初始值
    val counter = Option(counterState.value()).getOrElse(EventCounter(i.id, 0.0, 0))
    // 统计聚合
    counter.count += 1
    counter.sum += i.value
    // 发送结果到下游
    collector.collect(counter)
    // 保存状态
    counterState.update(counter)
  }
}
