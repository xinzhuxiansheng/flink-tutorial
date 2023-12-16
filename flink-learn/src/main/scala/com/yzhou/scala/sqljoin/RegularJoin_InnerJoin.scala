package com.yzhou.scala.sqljoin

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.ZoneId

/**
 * 普通 Join（Regular Join) 之 Inner Join
 */
object RegularJoin_InnerJoin {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = TableEnvironment.create(settings)


    // 指定时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 订单表
    val userOrderTableSql =
      """
        |CREATE TABLE user_order(
        | order_id BIGINT,
        | ts BIGINT,
        | d_timestamp AS TO_TIMESTAMP_LTZ(ts,3)
        | -- 注意：d_timestamp的值可以从原始数据中取，原始数据中没有的话也可以从kafka的元数据中取
        | -- d_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
        |)WITH(
        |
        |)
        |""".stripMargin

    // 支付表

    // 结果表

  }
}
