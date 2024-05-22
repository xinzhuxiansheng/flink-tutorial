package com.yzhou.scala.upsertkafka

import com.yzhou.common.utils.FileUtil
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode

/*
  以 Kafka 作为源，按时 Table API 转 Retract 和 Upsert 流
  示例数据：{"name":"zs","age":19}
 */
object KafkaSource2DataStream {
  def main(args: Array[String]): Unit = {
    //由于需要将Table转为DataStream，所以需要使用StreamTableEnviroment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    //创建输入表
    val inTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/upsertkafka/kafka_source.sql");
    tEnv.executeSql(inTableSql)

    //业务逻辑
    val execSql =
      """
        |SELECT
        |  age,
        |  COUNT(*) AS cnt
        |FROM kafka_source
        |GROUP BY age
        |""".stripMargin

    //执行SQL查询操作
    val resTable = tEnv.sqlQuery(execSql)

    //将结果转换为DataStream数据流-Retract  回撤流
//    val resStream = tEnv.toChangelogStream(resTable,
//      Schema.newBuilder().build(),
//      ChangelogMode.all()
//    )

    //将结果转换为DataStream数据流-Upsert   Changelog流， 必须包含主键
    val resStream = tEnv.toChangelogStream(resTable,
      Schema.newBuilder().primaryKey("age").build(),
      ChangelogMode.upsert()
    )

    //打印DataStream数据流中的数据
    resStream.print()

    //执行
    env.execute("KafkaSourceSinkSQL1FixToDataStream")
  }

}
