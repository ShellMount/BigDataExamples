package com.setapi.sparkDemo.spark_mllib.classfication

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

/**
  * 基于Spark2.x中 DataFrame MLlib API 进行决策树算法预测森林植被
  * Created by ShellMount on 2020/1/23
  *
  **/

object CovTypePrecisionPipelineDemo {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("CovTypePrecisionPipelineDemo")
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
    // TODO: g. 预测: 应该 assembler 转换后，再预测。此处简易处理：直接预测
    val assemblerTestingData = assembler.transform(testingData)
    modelEvaluate(spark, model, assemblerTestingData)

    // TODO    TODO    TODO    TODO    TODO    TODO    TODO
    println("============================================")
    println("================   分割线   =================")
    println("============================================")

    /**
      * 使用 Pipeline 重写
      * 将前面所有的步骤封装到 PipeLine 中，进行实现
      * - 转换器
      * - 模型学习器
      * - 模型评估器
      * - 数据交差验证
      * - 模型调优
      */
    dtPipeLine(trainingData, testingData)


    // TODO    TODO    TODO    TODO    TODO    TODO    TODO
    println("============================================")
    println("================   分割线   =================")
    println("============================================")
    println("还原 one-hot 数据后进行训练")

    /**
      * 将原始数据中类别数据还原，不使用 类别 转换为二元向量数据进行训练
      */
    val uncodingData = unencodingOneHot(spark, trainingData)
    desionTreeWithUncodingData(spark, uncodingData)


    // 释放缓存
    trainingData.unpersist()
    testingData.unpersist()

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }


  // 使用 VectorIndexer 处理的模型
  private def desionTreeWithUncodingData(spark: SparkSession, uncodingData: DataFrame) = {
    val Array(uncodingTrainingData, uncodingTestingData) = uncodingData.randomSplit(Array(0.9, 0.1))
    uncodingTrainingData.cache()
    uncodingTestingData.cache()

    uncodingTrainingData.printSchema()
    uncodingTrainingData.show()

    // 合并多列数据到一个列中(属于向量Vector类型)
    val assembler = new VectorAssembler()
      .setInputCols(uncodingTrainingData.columns.filter(_ != "cover_type"))
      .setOutputCol("featureVector")


    // TODO: 使用 VectorIndexer 模型学习器对类别特征进行处理，不同于 one-hot 化的另一种特征处理方式
    val indexer = new VectorIndexer()
      .setMaxCategories(40) // 数据中有2个类别的特征数据，分别有4个特征、40个特征
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("indexerVector")

    // TODO: 使用决策树分类器进行处理
    val classifier = new DecisionTreeClassifier()
      // 设置随机种子
      .setSeed(Random.nextLong())
      // 设置最大的桶数（最大的特征为40）
      .setMaxBins(40)
      // 设置最大的深度
      .setMaxDepth(30)
      // 设置分类器的标签列
      .setLabelCol("cover_type")
      // 设置特征向量的列名
      .setFeaturesCol(indexer.getOutputCol)
      // 设置预测值的列名
      .setPredictionCol("prediction")

    // TODO: 创建管道
    val pipeline = new Pipeline()
      .setStages(Array(
        assembler,
        indexer,
        classifier
      ))

    // TODO: 使用数据训练模型
    val pipelineModel = pipeline.fit(uncodingTrainingData)

    // TODO: 使用模型评估
    val predictions = pipelineModel
      .transform(uncodingTestingData)
      .select("cover_type", "prediction", "probability")

    // TODO: h. 模型评估
    // 多分类评估器
    // 支持四类指标：param for metric name in evaluation
    // (supports `"f1"` (default), `"weightedPrecision"`,
    //   `"weightedRecall"`, `"accuracy"`)
    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("cover_type")
      .setPredictionCol("prediction")
    // 精确度
    val accuracy = evaluator2.setMetricName("accuracy").evaluate(predictions)
    // f1: 全称 F-Measure or F-Score, 常用于评价分类模型的好坏
    val f1 = evaluator2.setMetricName("f1").evaluate(predictions)
    println(s"模型预测的精确度: ${accuracy}  \t F1: ${f1}")


  }

  // 一个标准的Pipeline模型
  def dtPipeLine(trainingData: DataFrame, testingData: DataFrame): Unit = {

    // TODO: 转换数据: VectorAssembler
    val inputColumns = trainingData.columns.filter(_ != "cover_type")
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(inputColumns)
      .setOutputCol("features")

    // TODO: 模型学习器
    val dtClassifier = new DecisionTreeClassifier()
      // 设置随机种子
      .setSeed(Random.nextLong())
      // 设置分类器的标签列
      .setLabelCol("cover_type")
      // 设置特征向量的列名
      .setFeaturesCol(assembler.getOutputCol)
      // 设置预测值的列名
      .setPredictionCol("prediction")

    // TODO: 组合管道
    val pipeline = new Pipeline()
      .setStages(Array(
        assembler,
        dtClassifier
      ))

    // TODO: HyperParameters Tuning优化
    // 决策树超参数设置值
    val paramGid: Array[ParamMap] = new ParamGridBuilder()
      // 设置不纯度量方式
      .addGrid(dtClassifier.impurity, Array("entropy", "gini"))
      // 设置树深度
      .addGrid(dtClassifier.maxDepth, Array(10, 20, 30))
      // 设置桶数
      .addGrid(dtClassifier.maxBins, Array(16, 32, 64))
      //
      .addGrid(dtClassifier.minInfoGain, Array(0.01, 0.05))
      .build()

    // TODO: 创建评估器
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("cover_type")
      .setPredictionCol("prediction")

    // TODO: 交差训练: 训练与验证分离
    // TrainValidationSplit 本身也是一个模型学习器
    val trainValidationSplit = new TrainValidationSplit()
      .setSeed(Random.nextLong())
      // 设置模型学习器
      .setEstimator(pipeline)
      // 设置模型评估器
      .setEvaluator(evaluator)
      // 设置模型学习器参数
      .setEstimatorParamMaps(paramGid)
      // 设置训练数据占比
      .setTrainRatio(0.9)

    // TODO: 模型训练
    val validationSplitModel: TrainValidationSplitModel = trainValidationSplit.fit(trainingData)

    // 得到最好的模型
    val bestModel: Model[_] = validationSplitModel.bestModel
    // 看看最好模型的参数值
    val paramMap = bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap()
    println(s"最好模型的参数值: ${paramMap}")

    // TODO: 测试
    //    validationSplitModel.transform(testingData)
    //      .select("cover_type")
  }

  def modelEvaluate(spark: SparkSession, dtModel: DecisionTreeClassificationModel, assemblerTestingData: DataFrame): Unit = {
    import spark.implicits._
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

  def unencodingOneHot(spark: SparkSession, data: DataFrame): DataFrame = {

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.linalg.Vector
    // 自定义函数，UDF，完成 one-hot -> 类别的还原
    val unhotUDF = udf ((vector: Vector) => vector.toArray.indexOf(1.0).toDouble)

    // TODO: Wilderness_Area 类别数据处理: 还原
    val wildernessCols = (0 until 4)
      .map(index => s"wilderness_area_${index}")
      .toArray

    // 将四列数据合并到一列数据中
    val wildernessAssembler = new VectorAssembler()
      .setInputCols(wildernessCols)
      .setOutputCol("wilderness")

    val withWilderness: DataFrame = wildernessAssembler
      // 增加了一个新的列
      .transform(data)
      // 删除其它列
      .drop(wildernessCols: _ *)
      .withColumn("wilderness", unhotUDF($"wilderness"))

    // TODO: Soil_Type 类别数据处理：还原
    val soilCols: Array[String] = (0 until 40)
      .map(index => s"soil_type_${index}")
      .toArray

    // 将四列数据合并到一列数据中
    val soilAssembler = new VectorAssembler()
      .setInputCols(soilCols)
      .setOutputCol("soil")

    val withSoil: DataFrame = soilAssembler
      // 增加了一个新的列
      .transform(withWilderness)
      // 删除其它列
      .drop(soilCols: _ *)
      .withColumn("soil", unhotUDF($"soil"))

    // 返回
    withSoil
  }
}
