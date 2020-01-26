package com.setapi.sparkDemo.spark_mllib.rmd

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用MovieLens电影评分数据集，调用
  * Spark MLlib中协同过滤推荐算法ALS
  *   a. 预测用户对某个电影的评价
  *   b. 为用户推荐电影TOPN
  *   c. 为某个电影推荐一批用户TOPN
  *
  * 使用基于RDD的Spark MLlib API
  * Created by ShellMount on 2019/12/15
  *
  **/

object MovieALSRmd {
  def main(args: Array[String]): Unit = {

    // TODO: 构建SparkContext实例
    val sparkConf = new SparkConf()
      .setAppName("MovieALSRmd")
      .setMaster("local[3]")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")

    // TODO: 读取电影评分数据
    val rawRatingRDD = sc.textFile("file:///E:/APP/BigData/api/data/als/movielens/ml-100k/u.data")

    println(s"Count = ${rawRatingRDD.count()}")
    println(rawRatingRDD.first())

    // TODO: 数据转换，构建RDD[Rating]
    val ratingsRDD = rawRatingRDD
      .filter(line => line.length > 0 && line.split("\t").length == 4)
      .map {
        line => {
          // 字符串分割
          val Array(userId, movieId, rating, _) = line.split("\t")
          // 返回Rating实例对象
          Rating(userId.toInt, movieId.toInt, rating.toDouble)
        }
      }

    // TODO: 调用ALS算法中的显示训练函数训练模型(1000 * 100)
    // 特征值10， 迭代次数5
    val alsModel: MatrixFactorizationModel = ALS.train(ratingsRDD, rank = 10, iterations = 5)

    /**
      * 获取的模型内，包含了两个矩阵
      * a. 用户因子矩阵
      * b. 产品因子矩阵
      *
      */
    // userId -> Features
    val userFeatures = alsModel.userFeatures
    userFeatures.take(10).foreach(tuple => println(tuple._1 + "--->" + tuple._2.mkString(",")))

    println("---------------------")
    // productId -> Features
    val productFeatures = alsModel.productFeatures
    productFeatures.take(10).foreach(tuple => println(tuple._1 + "--->" + tuple._2.mkString(",")))

    // TODO: 几种预测结果
    // 预测用户对某个产品的评分
    val predictRating = alsModel.predict(196, 242)
    println(s"预测用户196 对电影242的评分：${predictRating}")

    // 为某个用户推荐10部电影
    val rmdMoives = alsModel.recommendProducts(196, 10)
    println("为用户 196 推荐的10部电影:")
    rmdMoives.foreach(println)

    // 为某部电影推荐10个用户
    val rmdUsers = alsModel.recommendUsers(242, 10)
    println("为电影 242 推荐的10个用户:")
    rmdUsers.foreach(println)

    // TODO: 将训练的模型保存，后期使用
    val path = "file:///E:/APP/BigData/api/data/models/movie-model-als/"
    // alsModel.save(sc, path)

    // TODO: 从文件系统中加载保存的模型，用于推荐
    val newModel = MatrixFactorizationModel.load(sc, path)
    val newPredictRating = newModel.predict(196, 242)
    println(s"用新模型预测用户196 对电影242的评分：${newPredictRating}")

    // TODO: 模型的评估
    val uprsRDD = ratingsRDD.map(tuple => ((tuple.user, tuple.product), tuple.rating))
    val predictUprsRDD = alsModel.predict(uprsRDD.map(_._1))
        .map(tuple => ((tuple.user, tuple.product), tuple.rating))
    val predictAndActual = predictUprsRDD.join(uprsRDD)
    val metrics = new RegressionMetrics(predictAndActual.map(_._2))

    println(s"RMSE = ${metrics.rootMeanSquaredError}")
    println(s"MSE = ${metrics.meanSquaredError}")



    // 等待WEB UI
    Thread.sleep(1000 * 100)

    // 关闭资源
    sc.stop()
  }
}
