package com.setapi.flink_demo

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector
import org.apache.log4j.{Level, Logger}

/**
  * 状态编程
  *
  * 检测两次温度变化，超过一定范围，告警
  *
  * Created by ShellMount on 2020/1/29
  *
  **/

object CheckPointDemo {
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

    // TODO: 设置stateBackend
    // 开启checkpoint
    env.enableCheckpointing(1000 * 60, CheckpointingMode.EXACTLY_ONCE) // 隔1秒保存一次快照
    // 设置rocksDb: TODO: 需要使用HDFS保存
    // env.setStateBackend(new RocksDBStateBackend("path"))
    // env.setStateBackend(new FsStateBackend(""))
    // TODO: checkpoint 容错机制配置
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(1000*2)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 同时运行checkpoint的数量, 间隔过小时会出现
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 两次 checkpoint 间隔时间, 与上一行参数冲突
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000*10)
    // 开启外部持久化, 即使JOB失败, 也不会清理掉 checkpoint  / 同步清除
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    // 配置重启策略: 重试3次，间隔时间500毫秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500))

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

    // TODO: 计算连续 2秒内的温度 2 次/连续上升，报警
    val processedStream = stream.keyBy(_.id)
      .process(new TempIncreaseAlert())
    //    processedStream.print("processedStream")

    // TODO: 计算连续 2 次温度变化超过 一定限时 报警
    val procesedStream2 = stream.keyBy(_.id)
      .process(new TempChangeAlert(10.0))

    procesedStream2.print("Alert:")

    // TODO: 用 FlatMap 实现上面的功能
    val procesedStream3 = stream.keyBy(_.id)
      .flatMap(new TempChangeAlert2(10.0))
    procesedStream3.print("Alert From FlatMapProcess:")

    // TODO: 用 flatMapWithState 实现上面的功能
    val procesedStream4 = stream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
      // 没有状态的情况下, 将当前的温度值存入状态
      case (input: SensorReading, None) => (List.empty, Some(input.temperature))
      // 有状态的情况下，与上次温度的值作比较，判断是否告警
      case(input: SensorReading, lastTemperature: Some[Double]) => {
        val diff = (input.temperature - lastTemperature.get).abs
        if (diff > 2.0) {
          (List((input.id, lastTemperature.get, input.temperature)), Some(input.temperature))
        } else {
          (List.empty, Some(input.temperature))
        }
      }
    }
    procesedStream4.print("procesedStream4")


    env.execute("CheckPointDemo")

  }
}


class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
  // 定义一个状态，保存传感器，上一个数据的温度值
  lazy val lastTemperature: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemperature", classOf[Double]))

  override def processElement(in: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val previousTemperature = lastTemperature.value()
    // 用当前的温度与上次作比较, 超过阈值, 输出告警信息
    val diff = (in.temperature - previousTemperature).abs
    if (diff > threshold) {
      out.collect((in.id, previousTemperature, in.temperature))
    }

    lastTemperature.update(in.temperature)
  }
}

class TempChangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  // 定义一个状态，保存传感器，上一个数据的温度值
  private var lastTemperature: ValueState[Double] = _

  // 初始化过程中处理
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    lastTemperature = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemperature", classOf[Double]))
  }

  override def flatMap(in: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val previousTemperature = lastTemperature.value()
    // 用当前的温度与上次作比较, 超过阈值, 输出告警信息
    val diff = (in.temperature - previousTemperature).abs
    if (diff > threshold) {
      out.collect((in.id, previousTemperature, in.temperature))
    }

    lastTemperature.update(in.temperature)
  }
}