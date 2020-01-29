package com.setapi.flink_demo

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  *
  * Created by ShellMount on 2020/1/26
  *
  **/

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件读取数据
    val inputPath = "E:\\APP\\BigData\\api\\data\\flink\\words.txt"

    // 离线数据的DataSet
    val inputDataSet = env.readTextFile(inputPath)

    // 切分数据得到 word, 然后做分组聚合
    import org.apache.flink.api.scala._
    val wordCountDataSet = inputDataSet
      // 打散
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()

  }
}
