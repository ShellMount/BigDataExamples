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
object OrderTimeoutWithoutCep2 {
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
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
    val orderResultStream = groupStream.process(new OrderPayMatch())

    // 7. sink: 控制台输出
    orderResultStream.print("timeout")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order monitor job")
  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    // 保存pay状态是否来过
    lazy val isPayedSate: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed-state", classOf[Boolean]))

    // 保存定时器的时间戳
    lazy val timerSate: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 读取状态
      val isPayed = isPayedSate.value()
      val timerTs = timerSate.value()

      // 根据事件的类型进行分类判断
      if (value.eventType == "create") {
        // 收到create事件
        if (isPayed) {
          // pay来过
          out.collect(OrderResult(value.orderId, "payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedSate.clear()
          timerSate.clear()
        } else {
          // pay没来过，等待pay的到来
          val ts = (value.eventTime + 15*60) * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerSate.update(ts)
        }
      } else if (value.eventType == "pay") {
        // 收到pay事件
        if (timerTs > 0) {
          // create来过
          if (timerTs > value.eventTime * 1000L) {
            // 定时器时间未到
            out.collect(OrderResult(value.orderId, "payed successfully"))
          } else {
            // 已经超时
            ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but timeout"))
          }
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedSate.clear()
          timerSate.clear()
        } else {
          // pay 先到了,更新状态，注册定时器等待create
          isPayedSate.update(true)
          // 等到water marker涨到当前
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
          timerSate.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 根据状态的值判断哪个数据没有来
      if (isPayedSate.value()) {
        // pay先到了，没有等到crate
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create record"))
      } else {
        // create到了，没有等到 pay
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }

      timerSate.clear()
      isPayedSate.clear()
    }
  }

}
