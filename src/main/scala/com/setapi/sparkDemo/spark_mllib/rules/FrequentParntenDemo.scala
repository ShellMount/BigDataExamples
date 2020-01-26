package com.setapi.sparkDemo.spark_mllib.rules

import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
  * 使用FP-growth算法获取频繁项集 frequent itemsets
  * 它的核心语义是：买了A的人同时也买B的频次。
  * 类似于啤酒与尿布的关联关系一样。
  * 业务逻辑：买了又买
  * Created by ShellMount on 2020/1/21
  *
  **/

object FrequentParntenDemo {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("TaxiClustering")
      .master("local[3]")
      .getOrCreate()

    // SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // TODO: 1. 读取CSV数据，首行为列名称
    val schema = StructType(
      Array(
        StructField("tid", StringType, true),
        StructField("lat", DoubleType, true),
        StructField("lon", DoubleType, true),
        StructField("time", StringType, true)
      )
    )

    val rawRDD = sc.textFile("file:///E:/APP/BigData/api/data/rules/sample_fpgrowth.txt", 3)

    // 将每行数据分割
    val transactionsRDD = rawRDD.mapPartitions(iter => {
      iter.map(line => line.split("\\s+"))
    })
    transactionsRDD.cache().count()


    // TODO: 2. 使用FP-Growth 算法训练模型，找到频繁项集
    val fpg = new FPGrowth()
      // 设置最小的支持度
      .setMinSupport(0.5)
      // 设置分区数量
      .setNumPartitions(3)

    // 针对训练数据训练模型
    val fpgModel: FPGrowthModel[String] = fpg.run(transactionsRDD)

    // TODO: 3. 查看所有的频繁项集，并列出出现的次数
    val freqItemsetsRDD: RDD[FPGrowth.FreqItemset[String]] = fpgModel.freqItemsets
    println(s"Number of Frequent Items: ${freqItemsetsRDD.count()}")

    // 频繁项集：相邻ITEM同时出现的次数
    freqItemsetsRDD.filter(itemset => itemset.items.length > 1 && itemset.items.length < 4).foreach(println)

    /** 依据置信度，生成关联规则：设置最小的置信度
      * confidence: 在X发生的条件下，Y发生的概率, 此处为最小值，由此过滤
      * def confidence: Double = freqUnion / freqAntecedent
      *
      * class Rule[Item] private[fpm] (
      *
      * @Since("1.5.0") val antecedent: Array[Item],  // 表示前项，规则的假设：如，买A
      * @Since("1.5.0") val consequent: Array[Item],  // 表示后项，规则的结论：如，买A了又买B的
      *                 freqUnion: Double,            // 表示共同出现的次数
      *                 freqAntecedent: Double,       // 表示前项出现的次数
      *                 freqConsequent: Option[Double]) extends Serializable {
      */
    // 通过模型生成关联规则，设置最小置信度过滤数据
    val rulesRDD: RDD[Rule[String]] = fpgModel.generateAssociationRules(0.8)
    println(s"生成的规则数量：${rulesRDD.count()}")
    rulesRDD.foreach(println)

    // TODO: 4. 依据生成的关联规则，针对业务，得到推荐列表，进行推荐
    // 买了又买
    val rmdItemsRDD = rulesRDD.mapPartitions(iter => {
      iter.map(rule => (rule.antecedent.mkString(","), (rule.consequent.mkString(","), rule.confidence)))
    })
      // 使用聚合函数: 好复杂？？这是什么奇技淫巧，可以直接groupbykey?
      .aggregateByKey(ListBuffer[(String, Double)](), 2)(
      // 分区内聚合：seqOp: (U, V) => U
      (u, v) => {
        u += v
        u.sortBy(-_._2).take(5)
      },
      // 分区间聚合：combOp: (U, U) => U
      (u1, u2) => {
        u1 ++= u2
        u1.sortBy(-_._2).take(5)
      }
    )

    // 查看推荐的结果
    rmdItemsRDD.foreachPartition(iter => {
      iter
        .filter(_._1.split(",").length == 1)
        .foreach(item => {
          println(s"买(看)了此Item[${item._1}] 的人又买(看)了 Item[${
            item._2.foreach {
              case (rmdItem, confidence) => println(s"\t${rmdItem} -> ${confidence}")
            }
          }]")
        })
    })


    transactionsRDD.unpersist()

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
