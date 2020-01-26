package com.setapi.sparkDemo.spark_mllib.rmd

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * Created by ShellMount on 2019/12/26
  * 基于用户对艺术家 音乐播放的次数作为评价
  * 次数越多表明对该艺术家的音乐越喜好
  *
  **/

object MusicAlsRmd {
  def main(args: Array[String]): Unit = {
    // Spark App 上下文对象
    val spark = SparkSession.builder()
      .appName("MusicAlsRmd")
      .master("local[4]")
      .getOrCreate()


    // TODO: 获取SparkContext实例对象
    val sc = spark.sparkContext

    // 设置日志级别
    sc.setLogLevel("WARN")

    // TODO: 读取音乐评分数据
    val rawUserArtistRDD: RDD[String] = sc.textFile("file:///E:/APP/BigData/api/data/als/lastfm/user_artist_data.txt", minPartitions = 4)

    println(s"数据总量: ${rawUserArtistRDD.count()}")
    println(s"数据总量: ${rawUserArtistRDD.first()}")

    // 信息统计
    println(s"用户总数: ${rawUserArtistRDD.map(_.split("\\s")(0)).distinct().count()}")
    println(s"艺术家总数: ${rawUserArtistRDD.map(_.split("\\s")(1)).distinct().count()}")

    // TODO: 读取artist_alias.txt 数据，艺术家别名数据
    val rawArtistAliasRDD: RDD[String] = sc.textFile("file:///E:/APP/BigData/api/data/als/lastfm/artist_alias.txt", minPartitions = 4)
    val artistAliasMap: collection.Map[Int, Int] = rawArtistAliasRDD.flatMap(line => {
      val tokens = line.split("\t")

      // 由于某些原因，有些艺术家只有一个ID，没有别名
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }).collectAsMap()

    // 广播变量Map集合
    val broadCastArtistAlias = sc.broadcast(artistAliasMap)

    // TODO: 得到ALS算法所需训练数据集
    val trainingRDD: RDD[Rating] = rawUserArtistRDD.map(line => {
      // 用户-艺术家-播放次数，字符串分割
      val Array(userId, artistId, count) = line.split("\\s").map(_.toInt)

      // 将艺术家的ID统一化
      val finalArtistId = broadCastArtistAlias.value.getOrElse(artistId, artistId)

      // 返回 Rating 对象
      Rating(userId, artistId, count)
    })

    // 缓存数据，因为多次训练时从内存中读取
    trainingRDD.cache()

    // TODO: 使用ALS训练数据集训练模型
    val model: MatrixFactorizationModel = ALS.train(trainingRDD, 10, 5, 0.01)

    // 用户特征矩阵
    println("用户特征矩阵: " + model.userFeatures.first()._2.mkString(","))

    // 产品特征矩阵
    println("产品特征矩阵:" + model.productFeatures.first()._2.mkString(","))


    // TODO: 调整参数获取最佳模型
    val evaluations: Array[(Double, MatrixFactorizationModel, Int, Int, Double)] = for {
      rank <- Array(5, 10, 20, 30, 50, 100)
      iterations <- Array(5)  // 该值过大，则失败
      lamdba <- Array(0.01, 0.1, 1)
    } yield {
      val als_model =  ALS.train(trainingRDD, rank, iterations, lamdba)
      val als_model2 =  ALS.trainImplicit(trainingRDD, rank, iterations, lamdba, 1.0)

      // 评估模型，计算RMSE
      (alsModelEvaluate(als_model, trainingRDD), als_model, rank, iterations, lamdba)

    }

    // 找到最佳模型，依据 rmse 升序排序，获取最值小模型
    val sortEvaluations = evaluations.sortBy(_._1)
    sortEvaluations.foreach(println)


    /**
      * 如果使用显式训练最佳的效果是多少：
      * 。。。
      * 如果使用隐式训练
      * 发现：对于迭代次数对于模型的影响不大
      */
    val bestModel= sortEvaluations(0)._2
    println(s"最佳模型：${bestModel}")


    // 等待WEB UI
    Thread.sleep(1000 * 100)

    // 关闭资源
    sc.stop()
  }

  /**
    * 评估ALS模型，返回RMSE
    * @param model
    * @return
    */
  def alsModelEvaluate(model: MatrixFactorizationModel, rdd: RDD[Rating]): Double = {
    // 获取每个用户-艺术家的ID
    val usersProductRDD =  rdd.map(rating => (rating.user, rating.product))

    // 预测值
    val pridictRDD: RDD[Rating] = model.predict(usersProductRDD)

    val uprsRDD = rdd.map(tuple => ((tuple.user, tuple.product), tuple.rating))
    val predictAndActual = pridictRDD
      .map(r => ((r.user, r.product), r.rating))
      // uprsRDD 不能直接使用 = 号后面的值替代
      .join(uprsRDD)
      .map{
        case((userId, productId), (predict, actual)) => (predict, actual)
      }

    val metrics = new RegressionMetrics(predictAndActual)
    metrics.rootMeanSquaredError
  }
}
