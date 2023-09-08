package com.yzhou.cdc.scala

import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Properties

/**
 * MySQL CDC 2 Console
 */
object FlinkMySQLSourceToPrint {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    //设置全局并行度为4
    env.setParallelism(4)

    //开启Checkpoint
    //注意：在使用MySQL CDC 2.x版本时，如果不开启Checkpoint，则只能读取全量(快照)数据，无法读取增量(binlog)数据
    env.enableCheckpointing(5000)

    val prop = new Properties()
    //Caused by: java.security.cert.CertificateNotYetValidException: NotBefore
    //原因是MySQL服务器的时间(2028年)和当前客户端机器的时间相差太大导致的
    prop.setProperty("useSSL", "false")

    //借助于MySQL CDC组件指定数据源
    val mySqlSource = MySqlSource
      .builder[String] //String：表示MySqlSource采集到的数据类型
      .hostname("localhost") //MySQL主机名或者IP
      .port(3306) //MySQL端口
      .username("root") //MySQL用户名，此用户需要对MySQL CDC监视的所有数据库具有所需的权限
      .password("12345678") //MySQL密码
      .databaseList("yzhou_test") //设置需要获取的数据库，这里可以接收多个字符串参数，如果需要同步整个数据库，需要将tableList设置为".*"
      .tableList(".*") //设置需要获取的表，这里也可以接收多个字符串参数，表名前面需要指定数据库名称
      .jdbcProperties(prop) //在这里可以指定JDBC URL中的一些扩展参数
      .serverId("5400-5403") //serverId需要全局唯一，默认会在5400-6400之间生成一个随机数。
      //建议用户手工指定，可以指定一个具体的整数或者是一个整数范围，但是要保证该整数范围必须大于等于Source的并行度
      //例如：Source并行度为4，则整数范围内至少要包含4个数值，5400-5403
      .serverTimeZone("Asia/Shanghai") //默认不需要指定，它会自动获取MySQL服务器时区，如果指定，也要和MySQL服务器时区保持一致
      .deserializer(new JsonDebeziumDeserializationSchema()) //指定数据反序列化类，可以将MySqlSource采集到的数据转换为指定格式
      .build()

    import org.apache.flink.api.scala._
    val text = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")

    text.print()

    env.execute("FlinkMySQLSourceToPrint")

  }
}
