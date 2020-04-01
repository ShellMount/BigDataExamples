package com.setapi.flink_user_behavior_analysis.pv_uv_analysis


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 计算UV： 使用Bloon过滤器
  * 大数据量下Set无法存储的实现
  */
object UvWithBloom {
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
      .map(record => ("dummyKey", record.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      // 不能在窗口关闭时才实现，需自定义触发计算
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())
    //.sum(1)


    // 4. sink: 控制台输出
    processedStream.print("uv count")
    env.execute("uv count bloon job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {

  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义scala，保存所有的userId，并去重
    var idSet = Set[Long]()
    // 搜集所有数据ID到set中
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }

    out.collect(UvCount(window.getEnd, idSet.size))
  }
}
