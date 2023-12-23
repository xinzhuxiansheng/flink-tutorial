package com.yzhou.scala.sqljoin

import com.yzhou.common.utils.FileUtil
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.ZoneId

/**
 * 时间区间 Join（Interval Join) 之 Inner Join
 */
object IntervalJoin_InnerJoin {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = TableEnvironment.create(settings)


    // 指定时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 订单表
    val userOrderTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/intervaljoin/userorder.sql")
    tEnv.executeSql(userOrderTableSql)

    // 支付表
    val paymentFlowTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/intervaljoin/paymentflow.sql")
    tEnv.executeSql(paymentFlowTableSql)

    // 结果表
    val resTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/intervaljoin/orderpayment.sql")
    tEnv.executeSql(resTableSql)

    // 关联订单表和支付表
    val joinSql =
      """
        |INSERT INTO order_payment
        |SELECT
        | uo.order_id,
        | uo.user_id,
        | pf.pay_money
        |FROM user_order AS uo
        |-- 这里使用 INNER JOIN 或者 JOIN 是一样的效果
        |INNER JOIN payment_flow AS pf ON uo.order_id = pf.order_id
        |-- 指定取值的时间区间 (前后10分钟)
        |AND uo.d_timestamp
        |  BETWEEN pf.d_timestamp - INTERVAL '10' MINUTE
        |  AND pf.d_timestamp + INTERVAL '10' MINUTE
        |""".stripMargin

    tEnv.executeSql(joinSql)
  }
}
