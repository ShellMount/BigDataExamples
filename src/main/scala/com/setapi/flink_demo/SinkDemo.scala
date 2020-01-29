package com.setapi.flink_demo

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

import org.apache.commons.httpclient.HttpHost
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.log4j.{Level, Logger}

/**
  * Sink 输出数据
  * Created by ShellMount on 2020/1/26
  *
  **/


object SinkDemo {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[SensorReading] = env.addSource(new SensorSource())
    val dataStream: DataStream[String] = inputStream.map(_.toString) // toString 方便序列化输出
    dataStream.print("inputStream")


    /**
      * sink to kafka: 常用的情况: 数据来自 Kafka, 再去向 Kafka
      */
    dataStream.addSink(new FlinkKafkaProducer[String]("192.168.0.213:9092", "FLINK-ITEMS", new SimpleStringSchema()))

    // TODO: 为何不成功?
    // dataStream.addSink(new FlinkKafkaProducer011[String]("192.168.0.213:9092", "FLINK-ITEMS", new SimpleStringSchema()))

    /**
      * sink to redis
      */
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.0.211")
      .setPort(6379)
      .build()
    inputStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))


    /**
      * sink to ES
      */
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    // TODO: 待完成: 当前POM未关联到ES依赖包

    /**
      * sink to Mysql/Jdbc
      */
    inputStream.addSink(new MyJdbcSink())



    env.execute("sink demo")

  }
}

class MyRedisMapper() extends RedisMapper[SensorReading] {
  // 定义保存数据到Redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器 id , 温度 保存成哈希表: HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  // 定义保存到Redis的Key
  override def getKeyFromData(t: SensorReading): String = t.id


  // 定义保存到Redis的Value
  override def getValueFromData(t: SensorReading): String =  t.temperature.toString
}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {
  // 定义Sql连接
  var conn: Connection = _
  // 预编译器
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化过程，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://192.168.0.211:3306/flink", "root", "birdhome")
    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?,?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  // 调用连接执行SQL
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // TODO: 不能执行父类invoke, 原因不明
    // super.invoke(value, context)

    // 执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    // 如果update没有查到数据，则执行插入
    if(updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  // 关闭连接与预编译
  override def close(): Unit = {
    super.close()
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}


