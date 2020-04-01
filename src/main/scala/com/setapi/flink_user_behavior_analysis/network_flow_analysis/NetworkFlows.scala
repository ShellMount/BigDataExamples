package com.setapi.flink_user_behavior_analysis.network_flow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.{AggregateFunction, IterationRuntimeContext, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 流量统计
  */
object NetworkFlows {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    /*val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.213:9092")
    properties.setProperty("group.id", "flink-consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")*/

    val dataStream = env.readTextFile("E:\\APP\\BigData\\api\\data\\flink\\apache.log")
      .map(record => {
        val dataArray = record.split(" ")
        // 定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(
          dataArray(0).trim,
          dataArray(1).trim,
          timestamp,
          dataArray(5).trim,
          dataArray(6).trim
        )
      })
      // 乱序数据定义TS
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) { // 延迟容忍 10 秒
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime // 毫秒不必再乘以1000
      })

    // 3. transform 处理数据
    val processedStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      // 迟到数据可更新
      .allowedLateness(Time.seconds(60))
      // 自定义预聚合、窗口输出
      .aggregate(new CountAgg(), new WindowResult())
      // 按照窗口分组
      .keyBy(_.windowEnd)
      // 取TopN
      .process(new TopHotUrls(20))

    // 4. sink: 控制台输出
    processedStream.print()

    env.execute("hot url job")
  }
}

/**
  * 自定义聚合：求和
  * 初始值
  * 计算值：累加
  * 结果：累加结果
  * 合并：多个分区的值累加
  */
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
  * 自定义窗口函数
  * 输出 UrlViewCount
  */
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

/**
  * 求排序输出TopN
  *
  * @param topSize
  */
class TopHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  // 也可在 open 阶段初始化
  lazy val itemState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1) // 延迟1毫秒触发
  }

  // 定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将所有 state 中的数据取出
    val allItems: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()

    val iter = itemState.get().iterator()
    while (iter.hasNext) {
      allItems += iter.next()
    }

    // 按照count大小排序
    val sortedItems = allItems.sortWith(_.count > _.count).take(topSize)

    // 清空状态: 在 close 时清空?
    itemState.clear()

    // 结果
    val result = new StringBuilder()
    result.append("时间: ")
      .append(new Timestamp(timestamp - 1))
      .append("\n")

    // 输出每一个商品的信息
    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      result.append("No.")
        .append(i + 1)
        .append(": ")
        .append("浏览量=")
        .append(currentItem.count)
        .append(" URL=")
        .append(currentItem.url)
        .append("\n")
    }

    result.append("=========================")

    out.collect(result.toString())
  }

  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

  override def open(parameters: Configuration): Unit = {

    // itemState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("itemState", classOf[UrlViewCount]))
  }

  override def close(): Unit = {
    itemState.clear()
  }
}
