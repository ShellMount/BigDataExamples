package com.setapi.flink_demo

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.log4j.{Level, Logger}

/**
  * Transform 算子
  * Created by ShellMount on 2020/1/26
  *
  **/


object TransformDemo {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    // 从文件中读取数据
    val inputPath = "E:\\APP\\BigData\\api\\data\\flink\\words.txt"
    val stream2 = env.readTextFile(inputPath)
    // stream2.print("stream2")

    // 基本转算
    stream2.flatMap(item => item.split("\\s+"))
      .map { word => (word, 1) }
      // .keyBy("id")
      .keyBy(0)
      .sum(1)
      .print().setParallelism(1)

    // 基本转换
    println("--------------")
    stream2.flatMap(item => item.split("\\s+"))
      .map { word => (word, 1) }
      .keyBy(0)
      .reduce((x, y) => (x._1, x._2 + y._2))
      .print()

    // 多流转换算子
    // 3. split
    val stream3 = env.addSource(new SensorSource())
    // stream3.print("stream3")

    val splitStream = stream3.split(data => if (data.temperature > 30) Seq("high") else Seq("low"))
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all: DataStream[SensorReading] = splitStream.select("high", "low")
//    high.print("3.high")
//    low.print("3.low")
//    all.print("3.all")

    // 4. Connect / coMap合流操作: 不同类型的合流
    val warning = high.map(data => (data.id, data.temperature))
    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)
    val coMapDataStream: DataStream[Product] = connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
    coMapDataStream.print("coMapDataStream")

    // 5. Union 合并多条流: 数据结构需要一样
    val unionDataStream: DataStream[SensorReading] = high.union(low)

    // 6. 函数类: 进行过滤操作, 其它函数类同, map,...
    high.filter(new MyFilter())


    env.execute("transform demo")

  }
}

class MyFilter extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

class MyReacherFilter extends RichFilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.contains("sensor_2")
  }
}

class MyReacherMap extends RichMapFunction[SensorReading, String] {
  override def map(in: SensorReading): String = {
    // ...
    "result"
  }

  override def open(parameters: Configuration): Unit = {
    val subTaskIndex = getRuntimeContext.getIndexOfThisSubtask

    // 增加一些初始化工作, 如建立一个HDFS连接, 但一般不在这里做

    // 最后再执行原本要执行的初始化
    super.open(parameters)
  }

  override def close(): Unit = super.close()
}



