package com.setapi.flink_demo

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.log4j.{Level, Logger}

/**
  * 处理函数：数据处理过程中的控制
  *
  * Created by ShellMount on 2020/1/26
  *
  **/


object ProcessFunctionDemo {
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

    // 输出原始数据
    // stream.print("input data")

    // TODO: ProcessFunction 用于控制处理数据过程中的控制
    stream.keyBy(_.id)
      .process(new MyProcess())

    // TODO: 计算连续 2秒内的温度 2 次/连续上升，报警
    val processedStream = stream.keyBy(_.id)
      .process(new TempIncreaseAlert())

    processedStream.print("processed")


    env.execute("ProcessFunctionDemo")

  }
}

class MyProcess() extends KeyedProcessFunction[String, SensorReading, String] {
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    // TODO: 注册定时器，各种定时器的注册
    context.timerService().registerEventTimeTimer(2000)
  }
}

// 监视数据中，连续温度上升
class TempIncreaseAlert() extends KeyedProcessFunction[String, SensorReading, String] {
  // 定义一个状态，保存传感器，上一个数据的温度值
  lazy val lastTemperature: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemperature", classOf[Double]))
  // 保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(s: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    // 取出上一次的存储值
    val previousTemperature = lastTemperature.value()
    val currentTimerTs = currentTimer.value()

    // 更新温度值
    lastTemperature.update(s.temperature)

    // 温度上升, 且未设置过定时器
    if (s.temperature > previousTemperature && currentTimerTs == 0) {
      val timerTs = context.timerService().currentProcessingTime() + 1000
      // 设置定时器
      context.timerService().registerProcessingTimeTimer(timerTs)
      // 更新当前计时器状态值
      currentTimer.update(currentTimerTs)
    } else if (previousTemperature > s.temperature || previousTemperature == 0.0) {
      // TODO: 温度下降，或第一条数据时
      // 删除定时器
      context.timerService().deleteProcessingTimeTimer(currentTimerTs)
      // 清除状态
      currentTimer.clear()
    }
  }

  // 处理事件: 报警信息
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    super.onTimer(timestamp, ctx, out)

    // 输出信息
    out.collect(s"${ctx.getCurrentKey}  --> 温度连续上升")

    // 清空状态信息
    currentTimer.clear()
  }
}
