package com.yzhou.scala.sqlextend

import com.yzhou.common.utils.FileUtil
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.{Duration, ZoneId}

/**
 * Flink SQL 设置 State TTL 策略
 */
object FlinkSQL_StateTTL {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = TableEnvironment.create(settings)


    // 指定时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 设置 State 中数据的生存时间为10秒（实际工作中建议设置为1小时级别，时间不要太短，也不要太长）
    //tEnv.getConfig.setIdleStateRetention(Duration.ofSeconds(10))
    tEnv.getConfig.set("table.exec.state.ttl","1 h")

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
        | uo.order_id,
        | uo.d_timestamp,
        | pf.pay_money
        |FROM user_order AS uo
        |-- 这里使用 LEFT JOIN 或者 LEFT OUTER JOIN 是一样的效果
        |LEFT JOIN payment_flow AS pf ON uo.order_id = pf.order_id
        |""".stripMargin

    tEnv.executeSql(joinSql)
  }
}
