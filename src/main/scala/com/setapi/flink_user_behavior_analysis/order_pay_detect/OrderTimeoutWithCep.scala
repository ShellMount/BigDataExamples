package com.setapi.flink_user_behavior_analysis.order_pay_detect

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 定单支付状态检测
  * 使用CEP作模式匹配
  */
object OrderTimeOutWithCep {
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

    // 4. 定义匹配模式：对乱序数据友好
    val paymentPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
        .followedBy("follow").where(_.eventType == "pay")
        .within(Time.minutes(15))

    // 5. 在事件流上应用模式
    val patternStream = CEP.pattern(groupStream, paymentPattern)

    // 6. 从patter stream 上应用 select function 抽出匹配的事件序列
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
    val resultStream = patternStream.select(orderTimeoutOutputTag, new PatternTimeoutSelect(), new OrderPaymentMatch())

    // 7. sink: 控制台输出
    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order monitor job")
  }
}

/**
  * 正常匹配的处理函数
  */
class OrderPaymentMatch() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    // 从MAP中按照名称取出对应的事件
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}

/**
  * 自定义超时事件序列处理函数
  */
class PatternTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], timestamp: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

