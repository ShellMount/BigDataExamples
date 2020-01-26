package com.setapi.sparkDemo.spark_mllib.rules

import org.apache.spark.mllib.fpm.{PrefixSpan, PrefixSpanModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 使用频繁序列挖掘算法PrefixSpan
  * 它的核心语义是：买了A的人中间买了几个其它物品后同时也买B的频次。
  * 类似于啤酒，奶粉与尿布的关联关系一样。
  * 业务逻辑：买了又买
  *
  * 参考文档： https://www.cnblogs.com/pinard/p/6340162.html
  *
  * Created by ShellMount on 2020/1/21
  *
  **/

object PrefixSpanDemo {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("PrefixSpanDemo")
      .master("local[3]")
      .getOrCreate()

    // SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // TODO: 1. 读取CSV数据，首行为列名称
    val rawRDD: RDD[Array[Array[Int]]] = sc.parallelize(
      Seq(
        Array(Array(1), Array(1, 2, 3), Array(1, 3), Array(4), Array(3, 6)),
        Array(Array(1, 4), Array(3), Array(2, 3), Array(1, 5)),
        Array(Array(5, 6), Array(1, 2), Array(4, 6), Array(3), Array(2)),
        Array(Array(5), Array(7), Array(1, 6), Array(3), Array(2), Array(3))
      ),
      2 // 设置RDD分区数
    )
    rawRDD.cache()

    // TODO: 2. 构建PrefixSpan实例对象，设置最小置信度
    val prefixSpan = new PrefixSpan()
        .setMinSupport(0.5)     // 设置最小置信度
        .setMaxPatternLength(5) // 最大匹配长度

    // TODO: 3. 训练得到模型
    val prefixSpanModel: PrefixSpanModel[Int] = prefixSpan.run(rawRDD)

    // TODO: 4. 获取频繁序列
    val freqSequencesRDD: RDD[PrefixSpan.FreqSequence[Int]] = prefixSpanModel.freqSequences
    freqSequencesRDD.collect().foreach(
      freqSequence => {
        println(
          freqSequence.sequence.map(_.mkString("(", "|", ")")).mkString("<", ",", ">")
          + "  ->  " + freqSequence.freq
        )
      }
    )

    // TODO: 5. 依赖业务，设置 frequency >= 3, 模式长度等于2
    println("==============用于推荐的序列================")
    freqSequencesRDD
      .filter(freqSequence => {
        freqSequence.freq >= 3 && freqSequence.sequence.length == 2
      })
      .collect().foreach(
      freqSequence => {
        println(
          freqSequence.sequence.map(_.mkString("(", "|", ")")).mkString("<", ",", ">")
            + "  ->  " + freqSequence.freq
        )
      }
    )

    // TODO: 6. 保存模型: 生产中，需要将结果以 key-value 的方式存存储下来
    prefixSpanModel.save(sc, "")
    // 生产中，保存结果
    freqSequencesRDD.saveAsTextFile("")


    rawRDD.unpersist()

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
