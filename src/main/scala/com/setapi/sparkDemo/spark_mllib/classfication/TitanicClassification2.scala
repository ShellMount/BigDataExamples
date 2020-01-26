package com.setapi.sparkDemo.spark_mllib.classfication

import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.sql.SparkSession

/**
  * 使用Spark MLlib中分类算法，针对泰坦尼克数据集进行生存预测
  * Created by ShellMount on 2020/1/5
  *
  **/

object TitanicClassification2 {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("TitanicClassification2")
      .master("local[3]")
      .getOrCreate()

    // SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    /**
      * TODO: a. 读取Titanic数据集
      */
    val titanicDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file:///E:/APP/BigData/api/data/classify/titanic/train.csv")
    titanicDF.show(60, truncate = false)

    /**
      * TODO: b. 特征工程，提取特征值，组合到标签向量中
      */
    // 获取所有乘客年龄的平均值
    val aveAge = titanicDF.select("Age").agg("Age" -> "avg").first().getDouble(0)

    // 获取性别Sex集合映射
    val sexCategoryMap = titanicDF
      .select("Sex")
      .rdd
      .map(row => row.getString(0))
      .distinct()
      .zipWithIndex()
      .collectAsMap()
    // 广播性别集合
    val sexCategoryMapBroadcast = sc.broadcast(sexCategoryMap)

    val titanicRDD = titanicDF
      .select("Survived", "Pclass", "Sex", "Age", "SibSp", "Parch", "Fare")
      .rdd
      .map(row => {
        // 获取一个标签
        val label = row.getInt(0).toDouble

        // TODO: 性别处理: 把Sex变量转换为数字
        val sexFeatureArray = new Array[Double](sexCategoryMapBroadcast.value.size)
        sexFeatureArray(sexCategoryMapBroadcast.value(row.getString(2)).toInt) = 1.0
        // val sexFeature = if ("male".equals(row.getString(2))) 1.0 else 0.0

        // TODO: Age处理
        val ageFeature = if (row.get(3) == null) aveAge else row.getDouble(3)

        // 获取特征值
        val features = Vectors.dense(
          Array(row.getInt(1).toDouble,
            ageFeature,
            row.getInt(4).toDouble,
            row.getInt(5).toDouble,
            row.getDouble(6))
            ++ sexFeatureArray    // TODO: 类别特征的处理，避免使用 1, 2, 3, ... 来表示类型，而使用 One-Hot 特征
        )

        // 返回标签向量
        LabeledPoint(label, features)
      })

    // 划分数据集
    val Array(trainRDD, testRDD) = titanicRDD.randomSplit(Array(0.8, 0.2))

    /**
      * TODO: c.使用二分类算法训练模型: SVM/LR/DT/RF/GBT
      */

    // TODO: SVM:
    val svmModel = SVMWithSGD.train(trainRDD, 100)
    val svmScoreAndLabel = testRDD.map {
      case LabeledPoint(label, features) => (svmModel.predict(features), label)
    }
    // 二分类的评估
    val svmMetrics = new BinaryClassificationMetrics(svmScoreAndLabel)
    // 二分类评估指标：ROC面积，或召回率, PR面积
    println(s"使用SVM预测评估ROC：${svmMetrics.areaUnderROC()}")


    // TODO: LR:
    val lrModel = LogisticRegressionWithSGD.train(trainRDD, 100)
    val lrScoreAndLabel = testRDD.map {
      case LabeledPoint(label, features) => (lrModel.predict(features), label)
    }
    // 二分类的评估
    val lrMetrics = new BinaryClassificationMetrics(lrScoreAndLabel)
    // 二分类评估指标：ROC面积，或召回率, PR面积
    println(s"使用LR预测评估ROC：${lrMetrics.areaUnderROC()}")


    // TODO: DT:
    val dtModel = DecisionTree.trainClassifier(trainRDD,
      2,
      Map[Int, Int](),
      "gini", // 居然不能使用单引号, 不解!
      5,
      8)
    val dtScoreAndLabel = testRDD.map {
      case LabeledPoint(label, features) => (dtModel.predict(features), label)
    }
    // 二分类的评估
    val dtMetrics = new BinaryClassificationMetrics(dtScoreAndLabel)
    // 二分类评估指标：ROC面积，或召回率, PR面积
    println(s"使用DT预测评估ROC：${dtMetrics.areaUnderROC()}")


    // TODO: RFC:
    val rfModel = RandomForest
      .trainClassifier(
        trainRDD,
        2,
        Map[Int, Int](),
        10,
        "sqrt",
        "gini",
        5,
        8
      )
    val rfScoreAndLabel = testRDD.map {
      case LabeledPoint(label, features) => (rfModel.predict(features), label)
    }
    // 二分类的评估
    val rfMetrics = new BinaryClassificationMetrics(rfScoreAndLabel)
    // 二分类评估指标：ROC面积，或召回率, PR面积
    println(s"使用RF预测评估ROC：${rfMetrics.areaUnderROC()}")


    // TODO: GBDT: 梯度提长集成学习算法训练模型和预测
//    val gbtModel = GradientBoostedTrees.train(
//      trainRDD,
//      BoostingStrategy(
//        new Strategy(
//          Algo.Classification,
//          Gini,
//          2,
//          2,
//          4
//        ),
//        SquaredError
//      )
//    )
//
//    val gbtScoreAndLabel = testRDD.map {
//      case LabeledPoint(label, features) => (gbtModel.predict(features), label)
//    }
//    // 二分类的评估
//    val gbtMetrics = new BinaryClassificationMetrics(gbtScoreAndLabel)
//    // 二分类评估指标：ROC面积，或召回率, PR面积
//    // TODO: GBDT 未成功
//    // println(s"使用GBDT预测评估ROC：${gbtMetrics.areaUnderROC()}")




    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
