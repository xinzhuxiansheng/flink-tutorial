//package com.yzhou.scala.job
//
//import org.apache.flink.api.scala.createTypeInformation
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.table.api.{DataTypes, Schema, Table}
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.types.Row
//
//object Table2Stream {
//  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//  val tableEvn: StreamTableEnvironment = StreamTableEnvironment.create(env)
//
//  val inputTable: DataStream[String] = env.fromElements("Alice", "Bob", "John")
//
//  // 默认临时表的字段名(默认f0,f1,...)和字段类型皆来自源Datastream
//  tableEvn.createTemporaryView("InputTable", inputTable)
//
//  // 用户自定义字段名及字段类型，如下面的自定义schema
//  val schema = Schema.newBuilder().column("name", DataTypes.STRING()).build()
//  // tableEvn.createTemporaryView("InputTable2", inputTable, schema) // 这种shema怎么用呢？
//
//  val resultTable: Table = tableEvn.sqlQuery("select * from InputTable")
//
//  // 也可以从已定义Table创建临时表
//  tableEvn.createTemporaryView("InputTable3", resultTable)
//  val resultTable3: Table = tableEvn.sqlQuery("select * from InputTable3")
//
//  val resultStream: DataStream[Row] = tableEvn.toDataStream(resultTable)
//  val resultStream3: DataStream[Row] = tableEvn.toDataStream(resultTable3)
//
//  resultStream.print()
//  resultStream3.print()
//
//  env.execute()
//}
