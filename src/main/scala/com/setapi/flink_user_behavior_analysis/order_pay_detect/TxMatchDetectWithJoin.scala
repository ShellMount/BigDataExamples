package com.setapi.flink_user_behavior_analysis.order_pay_detect

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 对账
  * 使用流JOIN
  */
object TxMatchDetectWithJoin {
  // 侧输出流
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取数据: 定单流，与收据流
    val orderResource = getClass.getResource("/").toString.replace("target/classes/", "") + "data/flink/OrderLog.csv"
    val orderDataStream = env.readTextFile(orderResource)
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
    val orderGroupStream = orderDataStream
      .filter(_.txId != "")
      .keyBy(_.txId)

    // 4.支付到账流
    val receiptResource = getClass.getResource("/").toString.replace("target/classes/", "") + "data/flink/ReceiptLog.csv"
    val receiptDataStream = env.readTextFile(receiptResource)
      .map(record => {
        val dataArray = record.split(",")
        ReceiptEvent(
          dataArray(0).trim,
          dataArray(1).trim,
          dataArray(2).trim.toLong
        )
      })
      // 乱序数据: 指定时间戳与水位
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(5)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000L
      })

    // 5. transform 处理流
    val receiptGroupStream = receiptDataStream
      .keyBy(_.txId)

    // 6. 将两条流连接起来: JOIN, 不支持侧输出流
    val processStream = orderGroupStream.intervalJoin(receiptGroupStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processStream.print("matched")

    env.execute("tx match job")
  }

  /**
    * 同时出现事件A与事件B时的ACTION
    * 正常匹配的输出
    * 不能匹配未匹配的数据
    */
  class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      out.collect((left, right))
    }
  }
}
