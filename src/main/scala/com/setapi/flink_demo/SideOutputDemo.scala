package com.setapi.flink_demo

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.log4j.{Level, Logger}

/**
  * 侧输出流
  * Created by ShellMount on 2020/1/29
  *
  **/

object SideOutputDemo {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置event时间特征: 从调用时刻开始给env创建的每一个stream追加时间特征, 默认为 处理时的时间
    // processTime 是严格的按照时间流往后的 间隔，如10秒。而 eventTime 是事件中携带的时间的间隔，如10秒，有可能过了很久才等到这个10秒间隔
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 生成Watermark周期，默认200毫秒，另，processTime 时，默认为0
    env.getConfig.setAutoWatermarkInterval(200)

    // DataStream来源
    val stream = env.addSource(new SensorSource())
      // TODO: watermark 用于向所有的窗口通告，该水位线以下的数据都收到了，意味着窗口可以关闭或滚动了
      // 设置时间戳与水位线: 乱序数据
      .assignTimestampsAndWatermarks(
      // Time.milliseconds, 即bound, 延迟1秒发出,等待1秒: 如果 element.timestamp 是第5秒的数据来了, 那么水位为 4
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
        override def extractTimestamp(element: SensorReading): Long = {
          // 使用自带的时间戳作为事件时间
          element.timestamp
        }
      }
    )

    // 扩展的 split ，按照温度的高纸分组
    val processedStream = stream.process(new FreezingAlert())
    processedStream.getSideOutput(new OutputTag[String]("freezing alert"))
        .print("side flow")
    processedStream.print("major flow")


    env.execute("SideOutputDemo")

  }
}

// 冰点报警: 小于30度的温度，输出到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {
  import org.apache.flink.api.scala._
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      // 侧输出流
      context.output(alertOutput, s"freezing alert for ${value.id} --> ${value.temperature}")
    } else {
      // 主输出流
      collector.collect(value)
    }
  }
}
