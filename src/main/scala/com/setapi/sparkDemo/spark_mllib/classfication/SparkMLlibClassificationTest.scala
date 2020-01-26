package com.setapi.sparkDemo.spark_mllib.classfication

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * 基于RDD实现的分类算法
  * Created by ShellMount on 2020/1/4
  *
  **/

object SparkMLlibClassificationTest {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("SparkMLlibClassificationTest")
      .master("local[3]")
      .getOrCreate()

    // SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    /**
      * TODO: a. 读取鸢尾花数据集
      */
    val irisDF = spark.read.csv("file:///E:/APP/BigData/api/data/classify/iris.data")
    irisDF.show(60, truncate = false)

    /**
      * TODO: b. 提取特征值，组合到 Label-Point
      */
    // 提取出类型的值，转换为数值
    val categoryMap: collection.Map[String, Long] = irisDF
      .rdd
      // 获取所有类型并去重
      .map(row => row.getString(4)).distinct()
      .zipWithIndex()
      .collectAsMap()
    // 通过广播变量将Map广播到Executors中
    val categoryMapBroadcast = sc.broadcast(categoryMap)

    // 提取特征标签数据集
    val irisRDD: RDD[LabeledPoint] = irisDF
      .rdd
      .map(
        row => {
          // 创建稠密向量：Double类型的数组
          val features: linalg.Vector = Vectors.dense(
            Array(row.getString(0), row.getString(1), row.getString(2), row.getString(3)).map(_.toDouble)
          )

          // 获取标签
          val label = categoryMapBroadcast.value(row.getString(4)).toDouble

          // 返回标签-向量
          LabeledPoint(label, features)
        }
      )

    /**
      * TODO: 将数据分为两部分，训练、测试数据集
      */
    val Array(trainingRDD, testingRDD) = irisRDD.randomSplit(Array(0.8, 0.2))

    /**
      * TODO: 使用多分类算法，针对训练数据集进行训练
      */
    // LR: 逻辑回归
    //    val lrModel = LogisticRegressionWithSGD.train(trainingRDD, 10)
    //    // val lrModel = new LogisticRegressionWithLBFGS().run(trainingRDD)
    //    // 预测: 使用测试集进行预测
    //    val lrscoreAndLabels = testingRDD.map {
    //      // 返回预测值与真实值
    //      case LabeledPoint(label, features) => (lrModel.predict(features), label)
    //    }
    //    // 评估多分类模型
    //    val lrMetrics = new MulticlassMetrics(lrscoreAndLabels)
    //    println(s"使用逻辑回归的预计评估 acc = ${lrMetrics.accuracy}")


    // NB: 朴素贝叶斯
    // 算法要求每个特征值必须有值，非负数
    val nbModel = NaiveBayes.train(trainingRDD)
    val nbscoreAndLabels = testingRDD.map {
      // 返回预测值与真实值
      case LabeledPoint(label, features) => (nbModel.predict(features), label)
    }
    // 评估多分类模型
    val lrMetrics = new MulticlassMetrics(nbscoreAndLabels)
    println(s"使用贝叶斯回归的预计评估 acc = ${lrMetrics.accuracy}")

    // DT: 决策树
    /**
      * input: RDD[LabeledPoint], 训练数据
      * numClasses: Int,  分类的类别数量
      * categoricalFeaturesInfo: Map[Int, Int], 特征值中是如果有类型特征需要告知
      * impurity: String, 不纯度的度量方式，分类算法来说只有熵或基尼系数
      * maxDepth: Int,  数据的构建深度，默认为5
      * maxBins: Int 每个节点的分支数，为2的N次方
      */
    val dtModel = DecisionTree.trainClassifier(
      trainingRDD,
      3,
      Map[Int, Int](),
      "gini",   // 居然不能使用单引号, 不解!
      5,
      8
    )
    val dtscoreAndLabels = testingRDD.map {
      // 返回预测值与真实值
      case LabeledPoint(label, features) => (dtModel.predict(features), label)
    }
    // 评估多分类模型
    val dtMetrics = new MulticlassMetrics(dtscoreAndLabels)
    println(s"使用决策树的预计评估 acc = ${dtMetrics.accuracy}")

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
