package com.yzhou.cdc.scala.deserialization

import com.alibaba.fastjson.JSONObject
import com.ververica.cdc.debezium.DebeziumDeserializationSchema
import io.debezium.data.Envelope
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.util.Collector
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

class MyCustomerDeserializationSchema extends DebeziumDeserializationSchema[String]{

  /**
   * 反序列化方法
   * 注意：在这里需要根据实际业务需求，构造满足业务的数据格式
   * 例如：
   * {
   *   "db":"",
   *   "table":"",
   *   "before":{"field1":"value1","field2":"value2",...},
   *   "after":{"field1":"value1","field2":"value2",...},
   *   "op":""
   * }
   * @param sourceRecord
   * @param collector
   */
  override def deserialize(sourceRecord: SourceRecord, collector: Collector[String]): Unit = {
    //创建Json对象，封装结果数据
    val resObj = new JSONObject()

    //获取sourceRecord的数据格式
    //println(sourceRecord.toString)
    //SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1683364998, file=, pos=0}} ConnectRecord{topic='mysql_binlog_source.data.goods', kafkaPartition=null, key=Struct{id=3}, keySchema=Schema{mysql_binlog_source.data.goods.Key:STRUCT}, value=Struct{after=Struct{id=3,name=hammer,description=12oz carpenter's hammer},source=Struct{version=1.6.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,db=data,table=goods,server_id=0,file=,pos=0,row=0},op=r,ts_ms=1683364998821}, valueSchema=Schema{mysql_binlog_source.data.goods.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

    //获取数据库名称和表名称
    //mysql_binlog_source.data.goods
    val topic = sourceRecord.topic()
    val fields = topic.split("\\.")
    val db = fields(1)
    val table = fields(2)

    resObj.put("db",db)
    resObj.put("table",table)

    //获取before
    val value = sourceRecord.value().asInstanceOf[Struct]
    val before = value.getStruct("before")
    val beforeObj = new JSONObject()
    if(before !=null){//before字段有可能为空，例如：op=r/c这些情况
      val schema = before.schema()
      val beforeFields = schema.fields()//获取before里面的所有字段信息
      val it = beforeFields.iterator()
      while (it.hasNext){
        val field = it.next()
        //获取字段名称
        val fName = field.name()
        //获取字段值
        val fValue = before.get(fName)
        beforeObj.put(fName,fValue)
      }
    }

    resObj.put("before",beforeObj)

    //获取after
    val after = value.getStruct("after")
    val afterObj = new JSONObject()
    if(after != null){//after字段有可能为空，例如：op=d
      val schema = after.schema()
      val afterFields = schema.fields()
      val it = afterFields.iterator()
      while (it.hasNext){
        val field = it.next()
        //获取字段名称
        val fName = field.name()
        //获取字段值
        val fValue = after.get(fName)
        afterObj.put(fName,fValue)
      }
    }
    resObj.put("after",afterObj)

    //获取op
    val opType = Envelope.operationFor(sourceRecord)
    //建议把READ和CREATE操作统一成INSERT，并且所有操作名称全部转成小写
    var op = opType.toString.toLowerCase
    if(op.equals("read") || op.equals("create")){
      op = "insert"
    }
    resObj.put("op",op)

    //输出组装好的结果数据(把结果数据转成Json字符串输出)
    collector.collect(resObj.toJSONString)
  }

  /**
   * 指定返回的数据类型
   * @return
   */
  override def getProducedType: TypeInformation[String] = {
    BasicTypeInfo.STRING_TYPE_INFO
  }
}
