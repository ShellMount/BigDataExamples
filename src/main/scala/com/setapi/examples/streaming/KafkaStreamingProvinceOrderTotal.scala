package com.setapi.examples.streaming


import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, State, StateSpec, StreamingContext}
import org.slf4j.LoggerFactory

/**
  *
  * Created by ShellMount on 2019/7/31
  *
  **/

/**
  * 需求：求每个省份销售业绩
  *
  *
  * 模拟数据：
  * timestamp, provinceId, price
20171026164532001,12,345.00
20171026164532002,13,452.00
20171026164532011,12,251.00
20171026164532012,13,405.00
20171026164532021,7,145.00
20171026164532031,6,245.00
  * *
  * 20171026164532121,12,457.00
  * 20171026164532131,13,145.00
  * 20171026164532241,24,415.00
  * 20171026164532451,25,450.00
  *
  */

object KafkaStreamingProvinceOrderTotal {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)
  // it will store data on HDFS if hdfs-site.xml be offfered.
  // or local disk will be used.
  val CHECK_POINT_PATH = "/tmp/spark/streaming/my_checkpoint/ckpt-201707311753"

  def main(args: Array[String]): Unit = {
    //Logger.getRootLogger.setLevel(Level.WARN)

    // 通过检查点，来构建 StreamingContext
    val ssc = StreamingContext.getOrCreate(
      CHECK_POINT_PATH,
      () => {
        // SPARK APPLICATION
        val conf = new SparkConf().setMaster("local[2]").setAppName("MY_STREAMING_KAFKA")
        val context = new StreamingContext(conf, Durations.seconds(5))

        context.checkpoint(CHECK_POINT_PATH)
        // it should be here, but not out of getting.
        dataProcess(context)
        context
      }
    )

    // 设置日志
    ssc.sparkContext.setLogLevel("WARN")

    //启动实时流式应用，接收数据并分析
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  def dataProcess(ssc: StreamingContext) = {
    // KAFKA SETTING
    val kafkaParams = Map(
      "metadata.broker.list" -> "hdatanode2:9092",
      "auto.offset.reset" -> "largest")

    // TOPIC SETTING
    val topic = Set("MYFIRSTTOPIC")

    // Direct模式
    val kafkaDStream: InputDStream[(String, String)]
    = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topic
    )

    /**
      * 按照需求计算: 实时累加统计各省份销售订单
      * 这个算法，每个Duration都会输出结果
      * 不影响结果
      * 但会影起计算资源的浪费
      *
      * val provinceOrderTotalDStream = kafkaDStream.transform((rdd, time) => {
      * rdd.filter(msg => null != msg._2 && msg._2.trim.length > 0)
      * .mapPartitions(iter => {
      * 针对 每个分区数据操作: RDD 中每个分区数据对应于KAFKA TOPIC中每个分区的数据
      * iter.map(item => {
      * val Array(_, provinceId, orderPrice) = item._2.split(",")
      * 返回二元组，按照省份ID进行统计订单销售额，所以省份ID为KEY
      * (provinceId.toInt, orderPrice.toDouble)
      * })
      * })
      * }
      * ).updateStateByKey(
      * (values: Seq[Double], state: Option[Double]) => {
      * val previousState = state.getOrElse(0.0)
      * val currentState = values.sum
      * Some(previousState + currentState)
      * }
      * )
      */

    /**
      * 实时累加统计：mapWithState
      * 1，针对每一条数据进行数据
      * StateSpec:决定如何更新数据
      * 2，StateType: 所有的 Key->Value 的状态
      * 3，MpaaedType: 本批次数据处理之后，更新后的结果
      */
    val orderDStream: DStream[(Int, Double)] = kafkaDStream.transform(
      (rdd, time) => {
        rdd.filter(msg => null != msg._2 && msg._2.trim.length > 0)
          .mapPartitions(iter => {
            // 针对 每个分区数据操作: RDD 中每个分区数据对应于KAFKA TOPIC中每个分区的数据
            iter.map(item => {
              // TODO: 异常数据的处理。或数据清洗时需要注意
              if (!item._2.contains(" ")) {
                val Array(_, provinceId, orderPrice) = item._2.split(",")
                // 返回二元组，按照省份ID进行统计订单销售额，所以省份ID为KEY
                (provinceId.toInt, orderPrice.toDouble)
              } else {
                println("异常的数据: " + item._2)
                (0, 0.0)
              }
            })
          })
          // TODO: 在使用 mapWithState 时，更好的是：先聚合，再更新状态
          .reduceByKey(_ + _)
      })

    val mappingFunction: (Int, Option[Double], State[Double]) => (Int, Double)
    = (proviceId: Int, orderPrice: Option[Double], state: State[Double]) => {
      // 获取当前KEY对应已存的状态
      val previousState = state.getOption().getOrElse(0.0)
      // 获取本批次数据中的状态
      val currentState = previousState + orderPrice.getOrElse(0.0)
      // 更新状态值
      state.update(currentState)
      // 返回值
      (proviceId, currentState)
    }

    // 调用 mapWithState 实时计算状态
    val provinceOrderTotalDStream: DStream[(Int, Double)] = orderDStream.mapWithState(
      StateSpec.function[Int, Double, Double, (Int, Double)](mappingFunction)
    )

    // 输出结果
    provinceOrderTotalDStream.foreachRDD((rdd, time) => {
      println(s"Batch Time: ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS").format(new Date())}")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
      }
      println()
    })
    provinceOrderTotalDStream.print()
  }

}
