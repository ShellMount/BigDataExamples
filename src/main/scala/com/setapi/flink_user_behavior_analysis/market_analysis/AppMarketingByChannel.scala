package com.setapi.flink_user_behavior_analysis.market_analysis

import java.lang
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import java.util.{Random, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 分渠道统计市场推广数据
  */
object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val dataStream = env.addSource(new SimulatedEventSource1())
      // 升序数据直接指定TS
      .assignAscendingTimestamps(_.timestamp)

    // 3. transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior != "UNINSTALL")
      .map(record => ((record.channel, record.behavior), 1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel())

    // 4. sink: 控制台输出
    processedStream.print()

    env.execute("marketing job")
  }
}

/**
  * 自定义数据源
  */
class SimulatedEventSource1() extends RichSourceFunction[MarketingUserBehavior] {
  // 定义是否运行的标志位
  var running = true
  // 用户行为的集合
  val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 渠道集合
  val channelTypes: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 随机数发生器
  val rand: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 生成数据的上限
    val maxElements = Long.MaxValue
    var count = 0L

    // 随机生成所有数据
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelTypes(rand.nextInt(channelTypes.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))

      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = running = false
}

/**
  * 自定义的市场统计分析
  */
class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size

    out.collect(MarketingViewCount(startTs, endTs, channel, behavior, count))
  }
}

