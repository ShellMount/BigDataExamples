package com.setapi.flink_user_behavior_analysis.pv_uv_analysis

import com.setapi.flink_user_behavior_analysis.hot_items_analysis.{CountAgg, TopHotItems, WindowResult}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取数据
    val resource = getClass.getResource("/").toString.replace("target/classes/", "") + "data/flink/UserBehavior.csv"
    val dataStream = env.readTextFile(resource)
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
      // 升序数据直接指定TS
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior.equals("pv"))
      .map(record => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .sum(1)



    // 4. sink: 控制台输出
    processedStream.print("pv count")
    env.execute("pv count job")
  }
}
