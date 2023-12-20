package com.yzhou.scala.sqljoin

import com.yzhou.common.utils.FileUtil
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.ZoneId

/**
 * 普通 Join（Regular Join) 之 Left Join
 */
object RegularJoin_FullJoin {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = TableEnvironment.create(settings)


    // 指定时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 订单表
    val userOrderTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/regularjoin/userorder.sql")
    tEnv.executeSql(userOrderTableSql)

    // 支付表
    val paymentFlowTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/regularjoin/paymentflow.sql")
    tEnv.executeSql(paymentFlowTableSql)

    // 结果表
    val resTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/regularjoin/orderpayment_upsert.sql")
    tEnv.executeSql(resTableSql)

    // 关联订单表和支付表
    val joinSql =
      """
        |INSERT INTO order_payment
        |SELECT
        | case when pf.order_id is null then uo.order_id else pf.order_id end,
        | uo.d_timestamp,
        | pf.pay_money
        |FROM user_order AS uo
        |-- 这里使用 FULL JOIN 或者 FULL OUTER JOIN 是一样的效果
        |FULL JOIN payment_flow AS pf ON uo.order_id = pf.order_id
        |""".stripMargin

    tEnv.executeSql(joinSql)
  }
}
