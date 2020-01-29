package com.setapi.flink_demo

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.log4j.{Level, Logger}

/**
  * 时间戳与水位线
  * Created by ShellMount on 2020/1/26
  *
  **/


object WindowTimestampAndWaterMarkDemo {
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

      // TODO: 几种水位线与时间戳
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

      // 设置时间戳: 有序数据, 无需水位
      .assignAscendingTimestamps(_.timestamp)

      // 自定义周期性时间戳抽取
      .assignTimestampsAndWatermarks(new PeriodicAssigner())

      // 自定义乱序时间戳及水位线抽取
      .assignTimestampsAndWatermarks(new PunctuatedAssigner())


    // 统计10秒内每个传感器的最小温度
    val minTempPerWindowStream: DataStream[(String, Double)] = stream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      // TODO: 可以直接传WINDOW, 更底层
      // .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.hours(-8)))
      // TODO: 开时间窗口: 默认的时间是 process time,处理任务的时间，过短的数据流，会还没等到输出结果，就完成了
      // 隔5秒输出一次: 滑动窗口
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // 用reduce 作增量聚合

    // 每10秒计算一次每个传感器中最低温度: 受控于 TimeCharacteristic.EventTime 触发的时间
    minTempPerWindowStream.print("min temperature")

    // 输出原始数据
    stream.print("input data")


    env.execute("TimestampAndWaterMarkDemo")

  }
}

// 自定义周期性生成的水位线
class PeriodicAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound: Long = 60 * 1000 // 延迟为1分钟
  var maxTs: Long = Long.MinValue // 观察到的最大时间戳

  // 每200毫秒会调用一次该方法，更新水位线
  override def getCurrentWatermark: Watermark = {
    // TODO: 生成时间戳的规则
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timestamp)
    t.timestamp
  }
}

// 自定义乱序时间戳及水位线
class PunctuatedAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val bound: Long = 60 * 1000

  override def checkAndGetNextWatermark(t: SensorReading, extractedTs: Long): Watermark = {
    // TODO: 生成时间戳的规则
    if (t.id == "sensor_1") new Watermark(extractedTs - bound)
    else null
  }

  override def extractTimestamp(t: SensorReading, previousTs: Long): Long = {
    t.timestamp
  }
}
