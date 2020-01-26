package com.setapi.sparkDemo.spark_mllib.kmeans

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
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

object TaxiClusteringDF {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("TaxiClusteringDF")
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

    // TODO: 2. 合并多列数据到一个Vector中
    // 合并的列
    val columns = Array("lat", "lon")
    // 创建向量装配器
    val vectorAssembler = new VectorAssembler()
      // 设置输入的列名
      .setInputCols(columns)
      // 设置输出的列名
      .setOutputCol("features")

    // 转换数据
    val taxiFeaturesDF = vectorAssembler.transform(rawDF)
    taxiFeaturesDF.printSchema()
    taxiFeaturesDF.show()

    // 划分数据集
    val Array(trainingDF, testingDF) = taxiFeaturesDF.randomSplit(Array(0.7, 0.3), seed = 123)

    // TODO: 3. 将数据使用K-Means模型学习器进行训练学习得到模型
    // 创建K-Means模型学习器实例对象
    val km = new KMeans()
      .setK(10)       // 设置类簇中心点个数
      .setMaxIter(20) // 设置最大迭代次数
      .setFeaturesCol("features")     // 设置模型学习器使用数据的列名称
      .setPredictionCol("prediction") // 设置模型学习器得到模型以后预测数据值的列名称

    // 使用训练数据应用到模型学习器中
    val kmModel: KMeansModel = km.fit(trainingDF)

    // 获取聚簇的中心点
    val kmeansResult = kmModel.clusterCenters
    println(s"获取聚簇的中心点: ${kmeansResult.mkString(",")}")

    // TODO: 对测试数据
    val predictDF = kmModel.transform(testingDF)
    println("测试数据集的点属于哪些质点: ")
    predictDF.show()


    println(s"中心点ID=0的位置是: ${kmModel.clusterCenters(0)}")

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
