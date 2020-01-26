package com.setapi.sparkDemo.spark_mllib.pipeline

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 基于Spark2.0中SparkMLlib的PipeLine的快捷入门
  * -1. DataFrame
  * 所有的数据封装结构
  * -2. Transformer
  * 转换器: 将一个DataFrame调用算法/模型后，输出另一个新的DataFrame
  * -3. Estimator
  * 模型学习器: 也是一个算法, 输入 DataFrame 得到模型
  * -4. PipeLine
  * 管道: 由数据，转换器，模型学习器组成
  * -5. Parameter
  * 参数: 模型训练器、转换器过程中使用到的参数
  *
  * Created by ShellMount on 2020/1/19
  *
  *
  **/

/**
  * 案例说明：
  * 使用逻辑团成员归 对文本进行分类
  * 采用 TF 的方式提取文本特征
  *
  */

object PipeLineCrossValidationDemo {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("PipeLineCrossValidationDemo")
      .master("local[3]")
      .getOrCreate()

    // SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    // Prepare training documents from a list of (id, text, label) tuples.
    val trainingDF = spark.createDataFrame(
      Seq(
        (0L, "a b c d e spark", 1.0),
        (1L, "b d", 0.0),
        (2L, "spark f g h", 1.0),
        (3L, "hadoop mapreduce", 0.0)
      )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("rawFeatures")
    // 设置参数
    val params = ParamMap(hashingTF.numFeatures -> 500)

    // TODO: 对词袋模型BOW统计的上述TF进行加权处理 TF * IDF
    val idf = new IDF()
      .setInputCol(hashingTF.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    params.put(lr.regParam -> 0.02)

    // TODO: 参数设置与查看
    def setParam(): Unit = {
      val training = spark.createDataFrame(Seq(
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5))
      )).toDF("label", "features")

      val lr = new LogisticRegression()
      println(s"默认参数: ${lr.explainParams()}")

      // 实例参数设置
      lr.setMaxIter(20)
        .setRegParam(0.01)
      val model1 = lr.fit(training)
      println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap()}")

      // 通过ParamMap设置
      val paramMap = ParamMap(lr.maxIter -> 10).put(lr.maxIter -> 15).put(lr.regParam -> 0.55)

      val model2 = lr.fit(training, paramMap)
      println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap()}")

    }

    setParam()
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, idf, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(trainingDF)
    // val model = pipeline.fit(trainingDF, params)

    // Now we can optionally save the fitted pipeline to disk
    // model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    // pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
    // val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents.
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id, text, prob, prediction) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

    println("================ 如何得到最优模型 ==================")

    /**
      * TODO: Cross-Validation 交差验证
      * 设置K folds
      * 将数据集分为几个等分比如 3 folds，每次取 2/3 为训练集，1/3 为测试集
      * 共3种训练方式
      *
      * 也可以设置算法（逻辑回归）参数的不同值，训练模型，得到最优模型
      * 如设置 MaxIter = Array(10, 100, 1000)
      * 如设置 RegParam = Array(0.1, 0.01)
      *
      * 上述共计 3 * 3 * 2 种不同的训练
      */
    def crossValidation(): Unit = {
      val training = spark.createDataFrame(Seq(
        (0L, "a b c d e spark", 1.0),
        (1L, "b d", 0.0),
        (2L, "spark f g h", 1.0),
        (3L, "hadoop mapreduce", 0.0),
        (4L, "b spark who", 1.0),
        (5L, "g d a y", 0.0),
        (6L, "spark fly", 1.0),
        (7L, "was mapreduce", 0.0),
        (8L, "e spark program", 1.0),
        (9L, "a e c l", 0.0),
        (10L, "spark compile", 1.0),
        (11L, "hadoop software", 0.0)
      )).toDF("id", "text", "label")

      // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
      val hashingTF = new HashingTF()
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
      val lr = new LogisticRegression()
        .setMaxIter(10)
      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, hashingTF, lr))

      // We use a ParamGridBuilder to construct a grid of parameters to search over.
      // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
      // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
      val paramGrid = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
        .addGrid(lr.regParam, Array(0.1, 0.01))
        .build()

      // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
      // This will allow us to jointly choose parameters for all Pipeline stages.
      // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
      // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
      // is areaUnderROC.
      // TODO: CrossValidator 它是一个模型训练器
      val cv = new CrossValidator()
        .setEstimator(pipeline)
        // 由于是二分类，则传入二分类的评估器
        .setEvaluator(new BinaryClassificationEvaluator)
        // 设置交差验证的参数 K-Fold, LR算法参数
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(3)  // Use 3+ in practice
        .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

      // Run cross-validation, and choose the best set of parameters.
      val cvModel = cv.fit(training)

      // Prepare test documents, which are unlabeled (id, text) tuples.
      val test = spark.createDataFrame(Seq(
        (4L, "spark i j k"),
        (5L, "l m n"),
        (6L, "mapreduce spark"),
        (7L, "apache hadoop")
      )).toDF("id", "text")

      // Make predictions on test documents. cvModel uses the best model found (lrModel).
      cvModel.transform(test)
        .select("id", "text", "probability", "prediction")
        .collect()
        .foreach { case Row(id, text, prob, prediction) =>
          println(s"($id, $text) --> prob=$prob, prediction=$prediction")
        }
    }

    // 交差验证模型
    crossValidation()


    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
