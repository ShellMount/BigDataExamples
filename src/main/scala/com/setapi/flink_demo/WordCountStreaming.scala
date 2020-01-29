package com.setapi.flink_demo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 流处理WordCount
  * Created by ShellMount on 2020/1/26
  *
  **/

object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境: 流处理
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收一个 socket 文件流
    val inputDataStream = env.socketTextStream("192.168.0.211", 7777)

    // 切分数据得到 word, 然后做分组聚合r
    import org.apache.flink.api.scala._
    val wordCountDataStream = inputDataStream
      // 打散
      .flatMap(_.split("\\s+")).startNewChain()
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 执行结果，并设置并行度
    wordCountDataStream.print().setParallelism(2)

    // 启动 executor
    env.execute("SreamWordCountJob")

  }
}
