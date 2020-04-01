package com.setapi.flink_user_behavior_analysis.order_pay_detect

import com.setapi.flink_user_behavior_analysis.order_pay_detect.OrderTimeoutWithoutCep2.getClass
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
  * 对账
  * TODO: 模拟流一条条地输入，更准确。
  * 从文件中读取流的方式，可能存在差异
  */
object TxMatchDetect {
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

    // 6. 将两条流连接起来，共同处理，将以慢的记录的时间戳为作为水位检查
    val processStream = orderGroupStream.connect(receiptGroupStream)
      .process(new TxPayMatch())

    processStream.print("matched")
    processStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute("tx match job")
  }

  /**
    * 交易对账
    */
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    // 定义状态保存已经到达的订单支付事件、到账事件
    lazy val payState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    // 定单事件
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receipt = receiptState.value()
      if (receipt != null) {
        // 已经收过账了
        out.collect((pay, receipt))
        receiptState.clear()
      } else {
        // 还没到账，把pay存入状态，等一会儿
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer((pay.eventTime + 5) * 1000L)
      }
    }

    // 收账事件
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      if (pay != null) {
        // 已经支付过了
        out.collect((pay, receipt))
        payState.clear()
      } else {
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer((receipt.eventTime + 5) * 1000L)
      }
    }

    // 定时器触发
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 还没有收到某个事件，输出告警
      if (payState.value() != null) {
        // receipt没有收到
        ctx.output(unmatchedPays, payState.value())
      }

      if (receiptState.value() != null) {
        // pay没有支付
        ctx.output(unmatchedReceipts, receiptState.value())
      }

      // 清空
      payState.clear()
      receiptState.clear()
    }
  }
}
