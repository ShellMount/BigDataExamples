package com.setapi.flink_demo

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.log4j.{Level, Logger}

import scala.util.Random

/**
  * Source 数据源
  * Created by ShellMount on 2020/1/26
  *
  **/

// 传感器读数样例
case class SensorReading(
                        id: String,
                        timestamp: Long,
                        temperature: Double)

object SourceTest {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从调用时刻开始给env创建的每一个stream追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 从自定义的集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.889),
      SensorReading("sensor_2", 1547718213, 36.219),
      SensorReading("sensor_3", 1547718327, 35.189),
      SensorReading("sensor_4", 1547718433, 32.389),
      SensorReading("sensor_5", 1547718519, 31.488)
    ))

    stream1.print("stream1").setParallelism(1)


    // 2. 从文件中读取数据
    val inputPath = "E:\\APP\\BigData\\api\\data\\flink\\words.txt"
    val stream2 = env.readTextFile(inputPath)
    stream2.print("stream2")


    // 3. 直接读取
    env.fromElements(1, 2, "string").print("stream3")


    // 4. 从Kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.213:9092")
    properties.setProperty("group.id", "flink-consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream4 = env.addSource(new FlinkKafkaConsumer[String]("FLINK-ITEMS", new SimpleStringSchema(), properties))
    // stream4.print("stream4")


    // 5. 自定义Source
    val stream5 = env.addSource(new SensorSource())
    stream5.print("stream5")


    env.execute("source test")
  }
}

class SensorSource() extends SourceFunction[SensorReading] {
  // 定义一个flag, 表示数据源是否正常运行
  var running: Boolean = true

  // 正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()

    // 初始化定义一组传感器温度数据
    var currentTemp = 1 to 10 map(i => (s"sensor_${i}", 36 + rand.nextGaussian() * 10))

    // 用无限循环产生数据流
    while(running) {
      // 更新温度值
      val newTemperature = currentTemp.map(
        item => (item._1, item._2 + rand.nextGaussian())
      )

      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      newTemperature.foreach {
        case (id, temperature) => sourceContext.collect(SensorReading(id, curTime, temperature))
      }

      // 停一会儿
      Thread.sleep(1000)
    }

  }

  // 取消数据源的生成
  override def cancel(): Unit = {
    running = false
  }
}
