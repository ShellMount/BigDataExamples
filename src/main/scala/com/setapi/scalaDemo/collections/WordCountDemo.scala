package com.setapi.scalaDemo.collections

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  * 实现词频统计
  *
  **/

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    // 数据准备
    val lines = List(
      "spark, hadoop, hive, spark,hive",
      "spark, hadoop, hdfs, spark,hive",
      "spark, hadoop, sqoop, spark,hive",
      ", spark, hadoop, oozie, hue,hive"
    )

    // 数据转换
    val mappedWords = lines
      .flatMap(_.trim.split(",").toList)
      .map(_.trim)
      .filterNot(_.isEmpty)
      // 将单词转换为二元组
      .map((_, 1))

    // 数据分组
    val groupedTuple: Map[String, List[(String, Int)]] = mappedWords.groupBy(_._1)

    // 对每一组中的value值，进行统计
    val result = groupedTuple.map(tuple => {
      // 获取单词
      val word = tuple._1
      // 计算单词word对应的数量
      val count = tuple._2.map(_._2).sum
      (word, count)
    })

    // 输出结果
    result.toList.sortBy(- _._2).foreach(println)

  }
}
