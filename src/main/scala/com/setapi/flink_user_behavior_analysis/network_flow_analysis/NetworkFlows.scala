package com.setapi.flink_user_behavior_analysis.network_flow_analysis

import java.sql.Timestamp
import java.util.Properties

import com.setapi.flink_user_behavior_analysis.case_class.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.{AggregateFunction, IterationRuntimeContext, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 热门商品统计
  */
object HotItems {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.213:9092")
    properties.setProperty("group.id", "flink-consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // val dataStream = env.readTextFile("E:\\APP\\BigData\\api\\data\\flink\\UserBehavior.csv")
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("HotItems", new SimpleStringSchema(), properties))
      .map(record => {
        val dataArray = record.split(",")
        UserBehavior(
          dataArray(0).trim.toLong,
          dataArray(1).trim.toLong,
          dataArray(2).trim.toInt,
          dataArray(3).trim,
          dataArray(4).trim.toLong
        )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior.equals("pv"))
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 自定义预聚合、窗口输出
      .aggregate(new CountAgg(), new WindowResult())
      // 按照窗口分组
      .keyBy(_.windowEnd)
      // 取TopN
      .process(new TopHotItems(20))

    // 4. sink: 控制台输出
    processedStream.print()

    env.execute("hot items job")
  }
}

/**
  * 自定义聚合：求和
  * 初始值
  * 计算值：累加
  * 结果：累加结果
  * 合并：多个分区的值累加
  */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
  * 自定义聚合：平均值
  * 累加求和: Long
  * 数量计数: Int
  */
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
}

/**
  * 自定义窗口函数
  * 输出 ItemViewCount
  */
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

/**
  * 求排序输出TopN
  *
  * @param topSize
  */
class TopHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1) // 延迟1毫秒触发
  }

  // 定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将所有 state 中的数据取出
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()

    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }

    // 按照count大小排序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

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
        .append("商品ID=")
        .append(currentItem.itemId)
        .append(" 浏览量=")
        .append(currentItem.count)
        .append("\n")
    }

    result.append("=========================")

    out.collect(result.toString())
  }

  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount]))
  }

  override def close(): Unit = {
    itemState.clear()
  }
}
