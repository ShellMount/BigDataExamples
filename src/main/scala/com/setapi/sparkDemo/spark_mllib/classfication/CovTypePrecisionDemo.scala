package com.setapi.sparkDemo.spark_mllib.classfication

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

/**
  * 基于Spark2.x中 DataFrame MLlib API 进行决策树算法预测森林植被
  * Created by ShellMount on 2020/1/23
  *
  **/

object CovTypePrecisionDemo {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("CovTypePrecisionDemo")
      .master("local[3]")
      .getOrCreate()

    // SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    /**
      * TODO: a. 读取森林植被数据集
      */
    val covDF = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      // .schema(schema)
      .csv("file:///E:/APP/BigData/api/data/classify/森林植被数据/covtype.data")
    covDF.show(60, truncate = false)

    // TODO: b. 添加列名称
    val colNames = Seq(
      "Elevation".toLowerCase(),
      "Aspect".toLowerCase(),
      "Slope".toLowerCase(),
      "Horizontal_Distance_To_Hydrology".toLowerCase(),
      "Vertical_Distance_To_Hydrology".toLowerCase(),
      "Horizontal_Distance_To_Roadways".toLowerCase(),
      "Hillshade_9am".toLowerCase(),
      "Hillshade_Noon".toLowerCase(),
      "Hillshade_3pm".toLowerCase(),
      "Horizontal_Distance_To_Fire_Points".toLowerCase()
    ) ++ (0 until 4).map(index => "Wilderness_Area_".toLowerCase() + index) ++
      (0 until 40).map(index => "Soil_Type_".toLowerCase() + index) ++
      Seq("Cover_Type".toLowerCase())

    import spark.implicits._
    val dataDF = covDF.toDF(colNames: _*)
      .withColumn("cover_type", $"cover_type".cast("double"))

    dataDF.printSchema()
    dataDF.show()

    // TODO: c. 划分数据集
    val Array(trainingData, testingData) = dataDF.randomSplit(Array(0.9, 0.1))
    trainingData.cache()
    testingData.cache()

    // TODO: d. 把多列转为 1 个特征列: VectorAssembler
    // 获取所有的特征列
    val inputColumns = trainingData.columns.filter(_ != "cover_type")
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(inputColumns)
      .setOutputCol("featuresVector")

    // 数据的转换
    val assemblerTraningData: DataFrame = assembler.transform(trainingData)
    assemblerTraningData.printSchema()
    assemblerTraningData.show()

    // TODO: e. 训练模型: 构建 决策分类模型
    val classifier = new DecisionTreeClassifier()
      // 设置随机种子
      .setSeed(Random.nextLong())
      // 设置分类器的标签列
      .setLabelCol("cover_type")
      // 设置特征向量的列名
      .setFeaturesCol("featuresVector")
      // 设置预测值的列名
      .setPredictionCol("prediction")

    // 训练
    val model: DecisionTreeClassificationModel = classifier.fit(assemblerTraningData)

    // 打印模型的信息
    println("------------- 模型信息 -------------")
    println(model.toDebugString)

    // TODO: f. Desion Tree 中对每一个特征，都有一个重要性，以数值表示
    println("============= 特征重要性 ==============")
    model.featureImportances.toArray.zip(inputColumns).sorted.reverse.foreach(println)


    // TODO: 模型评估
    modelEvaluate(spark, model, assembler, testingData)

    trainingData.unpersist()
    testingData.unpersist()

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }


  def modelEvaluate(spark: SparkSession, dtModel: DecisionTreeClassificationModel, assembler: VectorAssembler, testingData: DataFrame): Unit = {
    import spark.implicits._
    // TODO: g. 预测: 应该 assembler 转换后，再预测。此处简易处理：直接预测
    val assemblerTestingData = assembler.transform(testingData)
    val predictions = dtModel.transform(assemblerTestingData)

    println("预测结果: ")
    predictions.select("cover_type", "prediction", "probability")
      .show()

    // TODO: h. 模型评估
    // 多分类评估器
    // 支持四类指标：param for metric name in evaluation
    // (supports `"f1"` (default), `"weightedPrecision"`,
    //   `"weightedRecall"`, `"accuracy"`)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("cover_type")
      .setPredictionCol("prediction")
    // 精确度
    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    // f1: 全称 F-Measure or F-Score, 常用于评价分类模型的好坏
    val f1 = evaluator.setMetricName("f1").evaluate(predictions)
    println(s"模型预测的精确度: ${accuracy}  \t F1: ${f1}")

    /**
      * 借助混淆矩阵进行评估
      */
    // 需要将 DataFrame 转换为 RDD
    val predictionRDD = predictions
      .select("cover_type", "prediction")
      .as[(Double, Double)] // 指定 DF 中 ROW的每个元素的类型，转换为 DataSet 数据类型
      .rdd // 不直接转换为RDD，是为了得到二元组
    val multiclassMetrics = new MulticlassMetrics(predictionRDD)
    println(s"多分类的混淆矩阵结果:  \n${multiclassMetrics.confusionMatrix}")
  }

}
