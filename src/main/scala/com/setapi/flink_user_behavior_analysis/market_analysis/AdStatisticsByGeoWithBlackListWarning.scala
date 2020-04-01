package com.setapi.flink_user_behavior_analysis.market_analysis

import java.sql.Timestamp

import com.setapi.flink_user_behavior_analysis.pv_uv_analysis.PageView.getClass
import com.setapi.flink_user_behavior_analysis.pv_uv_analysis.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 广告点击量数据统计
  * 黑名单过滤恶意点击
  * 包括清除过滤
  */
object AdStatisticsByGeoWithBlackListWarning {
  // 定义侧输出流TAG
  val blackListOutputTag = new OutputTag[AdBlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取数据
    val resource = getClass.getResource("/").toString.replace("target/classes/", "") + "data/flink/AdClickLog.csv"
    val dataStream = env.readTextFile(resource)
      .map(record => {
        val dataArray = record.split(",")
        AdClickEvent(
          dataArray(0).trim.toLong,
          dataArray(1).trim.toLong,
          dataArray(2).trim,
          dataArray(3).trim,
          dataArray(4).trim.toLong
        )
      })
      // 升序数据直接指定TS
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. 过滤大量刷广告点击行为
    val filterBlackListStream = dataStream
      .keyBy(record => (record.userId, record.adId))
      .process(new FilterBlackListUser(100))

    // 4. transform 处理数据
    val processedStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new AdCountAgg2(), new AdCountResult2())

    // 5. sink: 控制台输出
    processedStream.print("ad count")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blackList")
    env.execute("ad statistics job")
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    // 定义状态: 保存当前用户-广告为KEY的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent-black-list-state", classOf[Boolean]))
    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timer-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      // 取出countState
      val curCount = countState.value()

      // 第一次数据来的时候，需要注册定时器：每天0点触发
      if (curCount == 0) {
        val ts = (ctx.timerService().currentProcessingTime() / 1000 / 60 / 60 / 24 + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如达到，则加入黑名单
      if (curCount >= maxCount) {
        // 如已发送黑名单，则不发
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutputTag, AdBlackListWarning(value.userId, value.adId, s"过多的点击: ${maxCount}"))
        }
        return
      }

      // 计数状态加1
      countState.update(curCount + 1)

      // 主流输出
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }



}

class AdCountAgg2() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdCountResult2() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    out.collect(AdCountByProvince(window.getEnd, key, input.iterator.next()))
  }
}
