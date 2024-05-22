package com.yzhou.scala.upsertkafka

import com.yzhou.common.utils.FileUtil

import java.time.ZoneId
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode

object RegularJoin_LeftJoinToDataStream{
  /*
    Kafka 双流 Join，转 DataStream API

    示例数据
    订单表：
    {"order_id":1001,"ts":1665367200000}
    {"order_id":1002,"ts":1665367262000}

    支付表
    {"order_id":1002,"pay_money":100}
    {"order_id":1001,"pay_money":80}
   */
  def main(args: Array[String]): Unit = {
    //由于需要将Table转为DataStream，所以需要使用StreamTableEnviroment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    //指定国内的时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    //订单表
    val UserOrderTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/upsertkafka/user_order.sql");
    tEnv.executeSql(UserOrderTableSql)

    //支付表
    val PaymentFlowTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/upsertkafka/payment_flow.sql")
    tEnv.executeSql(PaymentFlowTableSql)

    //关联订单表和支付表
    val joinSql =
      """
        |SELECT
        |  uo.order_id,
        |  uo.d_timestamp,
        |  pf.pay_money
        |FROM user_order AS uo
        |-- 这里使用LEFT JOIN 或者LEFT OUTER JOIN 是一样的效果
        |LEFT JOIN payment_flow AS pf ON uo.order_id = pf.order_id
        |""".stripMargin
    val resTable = tEnv.sqlQuery(joinSql)

    //将结果转换为DataStream数据流-Retract
//    val resStream = tEnv.toChangelogStream(resTable,
//      Schema.newBuilder().build(),
//      ChangelogMode.all()
//    )

    //将结果转换为DataStream数据流-Upsert
      val resStream = tEnv.toChangelogStream(resTable,
        Schema.newBuilder().primaryKey("order_id").build(),
        ChangelogMode.upsert()
      )

    //打印 resStream 数据流中的数据
    resStream.print()

    //执行
    env.execute("RegularJoin_LeftJoinToDataStream")
  }
}
