package com.setapi.flink_user_behavior_analysis.order_pay_detect

import java.util

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 定单支付状态检测
  * 使用状态编程检测
  */
object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取数据
    val resource = getClass.getResource("/").toString.replace("target/classes/", "") + "data/flink/OrderLog.csv"
    val dataStream = env.readTextFile(resource)
      .map(record => {
        val dataArray = record.split(",")
        OrderEvent(
          dataArray(0).trim.toLong,
          dataArray(1).trim,
          dataArray(2).trim,
          dataArray(3).trim.toLong
        )
      })
      // 乱序数据: 指定时间戳与水位
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(5)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })

    // 3. transform 处理数据
    val groupStream = dataStream
      .keyBy(_.orderId)

    // 4. 定义process超时检测
    val timeoutWarningStream = groupStream.process(new OrderTimeoutWarning())

    // 7. sink: 控制台输出
    timeoutWarningStream.print("timeout")
    env.execute("order monitor job")
  }
}

/**
  * 定单超时告警
  */
class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  // 保存pay状态是否来过
  lazy val isPayedSate: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed-state", classOf[Boolean]))

  // 未作用户行为action的先后乱序列的处理
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    // 取出状态位
    val isPayed = isPayedSate.value()

    if (value.eventType == "create" && !isPayed) {
      // 如果遇到了create并且pay没来,注册定时器，并等待
      ctx.timerService().registerEventTimeTimer(value.eventTime*1000L + 15*60*1000L)
    } else if (value.eventType == "pay") {
      // 如果是pay事件，则更改状态为true
      isPayedSate.update(true)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 判断isPayed是否为true
    val isPayed = isPayedSate.value()
    if (isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }

    // 清空状态
    isPayedSate.clear()
  }
}
