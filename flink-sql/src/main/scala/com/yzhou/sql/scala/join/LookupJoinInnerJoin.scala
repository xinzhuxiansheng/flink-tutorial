package com.yzhou.sql.scala.join

import org.apache.flink.configuration.{Configuration, CoreOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.ZoneId

/**
 * 维表 Join（Lookup Join） Inner Join
 */
object LookupJoinInnerJoin {
  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())

    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(sEnv,settings)


    //创建执行环境
//    val settings = EnvironmentSettings
//      .newInstance()
//      .inStreamingMode()
//      .build()
//    val tEnv = TableEnvironment.create(settings)


    //设置全局并行度为1
    // tEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM.key(), "1")
    tEnv.getConfig.getConfiguration.set[Integer](CoreOptions.DEFAULT_PARALLELISM, 2);

    //指定国内的时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))


    //直播开播记录表
    val VideoDataTableSql =
      """
        |CREATE TABLE video_data(
        |  vid STRING,
        |  uid STRING,
        |  start_time BIGINT,
        |  country STRING,
        |  proc_time AS PROCTIME() -- 处理时间
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'yzhoutp01',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'properties.group.id' = 'gid-sql-video',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(VideoDataTableSql)

    //国家和大区映射关系-维表
    val CountryAreaTableSql =
      """
        |CREATE TABLE country_area(
        |  country STRING,
        |  area STRING
        |)WITH(
        |  'connector' = 'jdbc',
        |  'driver' = 'com.mysql.cj.jdbc.Driver', -- mysql8.x使用这个driver class
        |  'url' = 'jdbc:mysql://localhost:3306/yzhou_test?serverTimezone=Asia/Shanghai', -- mysql8.x中需要指定时区
        |  'username' = 'root',
        |  'password' = '12345678',
        |  'table-name' = 'country_area',
        |  -- 通过lookup缓存可以减少Flink任务和数据库的请求次数，启用之后每个子任务中会保存一份缓存数据
        |  'lookup.cache.max-rows' = '100', -- 控制lookup缓存中最多存储的数据条数
        |  'lookup.cache.ttl' = '3600000', -- 控制lookup缓存中数据的生命周期(毫秒)，太大或者太小都不合适
        |  'lookup.max-retries' = '1' -- 查询数据库失败后重试的次数
        |)
        |""".stripMargin
    tEnv.executeSql(CountryAreaTableSql)


    //结果表
    val resTableSql =
      """
        |CREATE TABLE new_video_data(
        |  vid STRING,
        |  uid STRING,
        |  start_time BIGINT,
        |  area STRING
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'yzhoutp02',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'format' = 'json',
        |  'sink.partitioner' = 'default'
        |)
        |""".stripMargin
    tEnv.executeSql(resTableSql)

    //关联开播记录表和国家大区关系表
    val joinSql =
      """
        |INSERT INTO new_video_data
        |SELECT
        |  vid,
        |  uid,
        |  start_time,
        |  area
        |FROM video_data
        |INNER JOIN country_area FOR SYSTEM_TIME AS OF video_data.proc_time
        |ON video_data.country = country_area.country
        |""".stripMargin
    tEnv.executeSql(joinSql)

  }
}
