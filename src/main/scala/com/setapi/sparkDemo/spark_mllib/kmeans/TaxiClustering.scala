package com.setapi.sparkDemo.spark_mllib.kmeans

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * 基于某市出租车行驶轨迹数据，使用K-Means算法进行聚类操作
  *
  * 数据说明：CSV
  * - 样本数据：
  * 1,30.624806,104.136604,211846
  * - 字段说明：
  * -- TID: 出租车ID
  * -- Lat: 出租车载客时的纬度坐标
  * -- Lon: 出租车载客时的经度坐标
  * -- Time: 记录时间戳 21:18:46
  *
  * Created by ShellMount on 2020/1/19
  *
  **/

object TaxiClustering {
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

    val rawDF = spark
      .read
      .schema(schema)
      .csv("file:///E:/APP/BigData/api/data/taxi/taxi.csv")


    rawDF.printSchema()
    rawDF.show()

    // TODO: 2. 提取特征
    val featuresRDD = rawDF
      .select("lat", "lon")
      .rdd
      .map(row => Vectors.dense(Array(row.getDouble(0), row.getDouble(1))))

    // 划分数据集
    val Array(trainingRDD, testingDD) = featuresRDD.randomSplit(Array(0.7, 0.3))

    // TODO: 3. 使用 K-Means 训练模型
    val km = KMeans.train(trainingRDD,
      10,     // K值
      20      //迭代次数
    )

    // 获取聚簇的中心点
    val kmeansResult: Array[linalg.Vector] = km.clusterCenters
    println(s"获取聚簇的中心点: ${kmeansResult.mkString(",")}")

    // TODO: 对测试数据
    val predictRDD = km.predict(testingDD)
    println("测试数据集的点属于哪些质点: ")
    predictRDD.take(200).foreach(println)


    println(s"中心点ID=0的位置是: ${km.clusterCenters(0)}")

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
