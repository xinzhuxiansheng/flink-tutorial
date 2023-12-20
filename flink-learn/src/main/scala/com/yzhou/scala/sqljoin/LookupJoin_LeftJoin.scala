package com.yzhou.scala.sqljoin

import com.yzhou.common.utils.FileUtil
import org.apache.flink.configuration.{Configuration, CoreOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.ZoneId

/**
 * 维表 Join（Lookup Join） Inner Join
 */
object LookupJoin_LeftJoin {
  def main(args: Array[String]): Unit = {
    val sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())

    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(sEnv, settings)


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
    val VideoDataTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/lookupjoin/videodata.sql")
    tEnv.executeSql(VideoDataTableSql)

    //国家和大区映射关系-维表
    val CountryAreaTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/lookupjoin/countryarea.sql")
    tEnv.executeSql(CountryAreaTableSql)


    //结果表
    val resTableSql = FileUtil.readFile("/Users/a/Code/Java/flink-tutorial/flink-learn/src/main/resources/join/lookupjoin/newvideodata.sql")
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
        |LEFT JOIN country_area FOR SYSTEM_TIME AS OF video_data.proc_time
        |ON video_data.country = country_area.country
        |""".stripMargin
    tEnv.executeSql(joinSql)

  }
}
