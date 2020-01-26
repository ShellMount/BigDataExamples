package com.setapi.sparkDemo.spark_mllib.regression

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * 使用Spark MLlib中回归算法预测共享单车每小时出租次数
  * 数据来源： www.kaggle.com/c/bike-sharing-demand/data
  * Created by ShellMount on 2020/1/19
  *
  **/

object BikeSharingLineRegression {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("BikeSharingLineRegression")
      .master("local[3]")
      .getOrCreate()

    // SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // TODO: 1. 读取CSV数据，首行为列名称
    val rawDF = spark
      .read
      .option("header", "true")
      //      .option("inferSchema", "true")
      .csv("file:///E:/APP/BigData/api/data/bikesharing/hour.csv")

    /**
      * root
      * |-- instant: integer (nullable = true)    序号
      * |-- dteday: timestamp (nullable = true)   日期，时间戳
      * |-- season: integer (nullable = true)     季节
      * |-- yr: integer (nullable = true)         年份： 2011=0， 2012=1
      * |-- mnth: integer (nullable = true)       月份
      * |-- hr: integer (nullable = true)         小时
      * |-- holiday: integer (nullable = true)    节假日：0, 1
      * |-- weekday: integer (nullable = true)    周几: 0, 1, 2, 3, 4, 5, 6
      * |-- workingday: integer (nullable = true) 工作日: 0, 1
      * |-- weathersit: integer (nullable = true) 天气
      * |-- temp: double (nullable = true)        气温        ： 经过正则化处理过的数据
      * |-- atemp: double (nullable = true)       体感温度     ： 经过正则化处理过的数据
      * |-- hum: double (nullable = true)         温度         ： 经过正则化处理过的数据
      * |-- windspeed: double (nullable = true)   风速         ： 经过正则化处理过的数据
      * |-- casual: integer (nullable = true)     未注册用户
      * |-- registered: integer (nullable = true) 注册用户
      * |-- cnt: integer (nullable = true)        租用自行车统计数量
      */
    rawDF.printSchema()
    rawDF.show()

    // TODO: 定义类别特征转数字
    /**
      * 将类别的特征转换为数值，数组表示（one-hot)
      * @param categoryMap  类别对应的Map集合
      *
      * @param categoryValue 某个类别值
      * @return
      */
    def transformCategory(categoryMap: collection.Map[String, Long], categoryValue: String): Array[Double] = {
      // 构建数组，长度为类别特征长度（有多少个类别）
      val categoryArray = new Array[Double](categoryMap.size)
      // 依据类别的值获取对应的Map集合中Value的值（index）
      val categoryIndex: Long = categoryMap(categoryValue)
      // 赋予对应数组下标的值为1.0
      categoryArray(categoryIndex.toInt) = 1.0
      // 返回数组
      categoryArray
    }

    // TODO: 2. 选取特征值，特征工程
    // a. 选取特征
    val recordsRDD = rawDF
      .select(
        // 8个类别特征: Int
        "season",
        "yr",
        "mnth",
        "hr",
        "holiday",
        "weekday",
        "workingday",
        "weathersit",
        // 4个数据特征: Double
        "temp",
        "atemp",
        "hum",
        "windspeed",
        // 标签值：预测值: Int
        "cnt"
      ).rdd

    // TODO: 获取以上 8 个类别特征对应Map集合映射 （类别 -> 索引）
    val categorMapMap: Map[Int, collection.Map[String, Long]] = (0 to 7).map(index => {
      // 分别对索引下标的值获取，构建Map集合映射
      val categoryMap = recordsRDD.map(row => row.getString(index))
        .distinct
        .zipWithIndex
        .collectAsMap()
      // 以二元组返回, 转换为Map集合
      (index, categoryMap)
    }).toMap

    // 广播变量
    val categoryMapMapBroadcast = sc.broadcast(categorMapMap)


    // 线性回归中，需要对类别特征进行转换
    val lpsRDD = recordsRDD.map(row => {
      // 获取广播变量的值
      val categorMapMapValue = categoryMapMapBroadcast.value

      val arrayBuffer = new ArrayBuffer[Double]()
      for (index <- 0 to 7) {
        arrayBuffer ++= transformCategory(categorMapMapValue(index), row.getString(index))
      }

      arrayBuffer += row.getString(8).toDouble
      arrayBuffer += row.getString(9).toDouble
      arrayBuffer += row.getString(10).toDouble
      arrayBuffer += row.getString(11).toDouble

      // 特征向量
      val features = Vectors.dense(arrayBuffer.toArray)

      // 返回标签向量
      LabeledPoint(row.getString(12).toDouble, features)
    })

    println("处理后的特征集：")
    lpsRDD.take(10).foreach(println)

    // 数据集划分为训练与测试集
    val Array(trainingRDD, testRDD) = lpsRDD.randomSplit(Array(0.8, 0.2), seed = 123L)

    // 缓存训练数据
    trainingRDD.cache()
    testRDD.cache()


    // b. 特征处理，类别处理，归一化，标准化，正则化，数值转换，one-hot化


    /**
      * 无论分类还是回归算法，数据集都是RDD[LabelPoint]
      * LabelPoint(label, features)
      */
    // TODO: 使用决策权回归算法训练模型, 决策树算法支持类别特征进行训练模型（即不用one-hot化也可以）
    val dtModel = DecisionTree.trainRegressor(
      trainingRDD,
      Map[Int, Int](
        0 -> 4, // 第 n 个特征类 有 k 个取值, 其取值从 0 开始： 0 ~ k-1
        1 -> 2,
        2 -> 12,
        3 -> 24,
        4 -> 2,
        5 -> 7,
        6 -> 2,
        7 -> 4
      ),
      "variance", // 回归模型中，仅有一种不纯度的度量方式，就是方差
      10, // 默认5，表示树的最大深度
      32 // 最大分支数, >= 类别特征的个数， 且为2的m次方
    )

    // 使用模型预测
    val predictAndActualRDD = testRDD.map(lp => {
      val predictLabel = dtModel.predict(lp.features)
      (predictLabel, lp.label)
    })

    println("真实值与预测值: ")
    println(predictAndActualRDD.take(10).foreach(println))

    // 回归模型的性能评估指标
    val metrics = new RegressionMetrics(predictAndActualRDD)
    println(s"决策树回归模型的评估，均方根误差RMSE: ${metrics.rootMeanSquaredError}")
    println(s"决策树回归模型的评估，均方误差MSE: ${metrics.meanSquaredError}")
    println(s"决策树回归模型的评估，平均平均绝对误差: ${metrics.meanAbsoluteError}")
    println(s"决策树回归模型的评估，R2: ${metrics.r2}")
    println(s"决策树回归模型的评估，Variance: ${metrics.explainedVariance}")

    /**
      * 模型优化
      * - 方法一：数据角度 - 特征数据, 增加、减少、优化
      * - 方法二：算法超参数角度 - 在特征值 一定的前提下，选取合适超参数组合
      * 推荐系统ALS已经使用过
      * 决策树算法适用于特征值更多的情况，表现会更好
      * 本版本中，减少了特征数量，效果也下降了。
      */

    println("================== 线性回归算法  ======================")

    /**
      * 使用特征数据不作转换时使用线性回归算法： 结果将毫无用处
      */
    val lrModel = LinearRegressionWithSGD.train(
      trainingRDD,
      100,
      0.1,
      0.8
    )


    // 使用模型预测
    val lrPredictAndActualRDD = testRDD.map(lp => {
      val predictLabel = lrModel.predict(lp.features)
      (predictLabel, lp.label)
    })

    println("线性回归模型真实值与预测值: ")
    println(lrPredictAndActualRDD.take(10).foreach(println))

    // 回归模型的性能评估指标
    val lrMetrics = new RegressionMetrics(lrPredictAndActualRDD)
    println(s"线性回归模型的评估，均方根误差RMSE: ${lrMetrics.rootMeanSquaredError}")
    println(s"线性回归模型的评估，均方误差MSE: ${lrMetrics.meanSquaredError}")
    println(s"线性回归模型的评估，平均平均绝对误差: ${lrMetrics.meanAbsoluteError}")
    println(s"线性回归模型的评估，R2: ${lrMetrics.r2}")
    println(s"线性回归模型的评估，Variance: ${lrMetrics.explainedVariance}")




    // 缓存训练数据
    trainingRDD.unpersist()
    testRDD.unpersist()

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
