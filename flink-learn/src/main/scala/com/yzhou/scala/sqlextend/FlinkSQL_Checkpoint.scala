package com.yzhou.scala.sqlextend

import com.yzhou.common.utils.FileUtil
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.ZoneId

/**
 * Flink SQL 设置 Checkpoint
 */
object FlinkSQL_Checkpoint {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

    // 开启Checkpoint
    env.enableCheckpointing(1000*10) // 为了便于观察方便，设置为10秒一次
    // 获取Checkpoint的配置对
    val cpConfig = env.getCheckpointConfig
    // 在任务故障和手工停止任务时 都会保留之前生成的Checkpoint数据
    cpConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    // 设置Checkpoint后的状态数据的存储位置
    cpConfig.setCheckpointStorage("file:///Users/a/TMP/flink_checkpoint");
    val tEnv = StreamTableEnvironment.create(env)
    // 指定时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 设置 State 中数据的生存时间为10秒（实际工作中建议设置为1小时级别，时间不要太短，也不要太长）
    //tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(10))

    val sourceTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/sqlextend/kafkasource.sql")
    tEnv.executeSql(sourceTableSql)

    val sinkTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/sqlextend/kafkasink.sql")
    tEnv.executeSql(sinkTableSql)

    // 关联订单表和支付表
    val joinSql =
      """
        |INSERT INTO kafka_sink
        |SELECT
        | age,
        | COUNT(*) AS cnt
        |FROM kafka_source
        |GROUP BY age
        |""".stripMargin

    tEnv.executeSql(joinSql)
  }
}
