package com.setapi.sparkDemo.spark_mllib.classfication

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel, Word2Vec}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 使用NB算法来预测新闻类别，分类算法
  * Created by ShellMount on 2020/1/18
  *
  **/

object NewsCategoryPredictNBTest {
  def main(args: Array[String]): Unit = {
    // TODO: 构建 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("NewsCategoryPredictNBTest")
      .master("local[3]")
      .getOrCreate()

    // SparkContext上下文对象
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // TODO: a. 读取制表符分割的csv数据，通过自定义Schema信息
    val schema = StructType(
      Array(
        StructField("id", IntegerType, true),
        StructField("title", StringType, true),
        StructField("url", StringType, true),
        StructField("publisher", StringType, true),
        StructField("category", StringType, true),
        StructField("story", StringType, true),
        StructField("hostname", StringType, true),
        StructField("timestamp", StringType, true)
      )
    )
    /**
      * TODO: b. 读取新闻数据集
      */
    val newsDF = spark
      .read
      .option("sep", "\t")
      .schema(schema)
      .csv("file:///E:/APP/BigData/api/data/classify/newsdata/newsCorpora.csv")
    newsDF.show(60, truncate = false)

    //TODO: 统计各个类别的数据，看看是否存在数据倾斜: 贝页斯不适合数据倾斜的场景
    newsDF.printSchema()
    newsDF
      .select("category")
      .groupBy("category")
      .count()
      .show(10, truncate = false)

    // TODO: 提取字段数据
    val titleCategoryRDD = newsDF
      .select("title", "category")
      .rdd
      .map(row => {
        // 获取类别数据
        val category = row.getString(1) match {
          case "b" => 0.0
          case "t" => 1.0
          case "e" => 2.0
          case "m" => 3.0
          case _ => -1
        }
        // 以二元组的形式返回
        (category, row.getString(0))
      })

    // TODO: 从新闻标题 title 提取特征值 (转换为数值类型)
    /**
      * 使用词袋模型(BOW)提取文本特征数据
      * TF-IDF
      */
    // TODO: 创建HashingTF, 指定单词/特征数量为100000
    val hashTF = new HashingTF(100000)

    // 特征转换
    val lpsRDD = titleCategoryRDD.map {
      case (category, newsTitle) => {
        // 对新闻标题进行分词
        val words = newsTitle.split("\\s+")
          // 单词小写
          .map(word => word.toLowerCase
          .replace(",", "")
          .replace(".", ""))
          .toSeq
        // 将每个文本分割的单词Seq转换为向量
        val tf = hashTF.transform(words)

        // 返回
        LabeledPoint(category, tf)

      }
    }.filter(lp => lp.label != -1)

    // 查看一下结果
    lpsRDD.take(5).foreach(println)

    // TODO: 获取IDF模型
    val tfRDD = lpsRDD.map(_.features)
    // 构建IDF: 通过TF值获取
    val idfModel: IDFModel = new IDF().fit(tfRDD)

    // TODO: 给TF加权
    val lpsTfIdfRDD = lpsRDD.map {
      case LabeledPoint(category, tf) => LabeledPoint(category, idfModel.transform(tf))
    }

    lpsTfIdfRDD.take(5).foreach(println)

    // TODO: 划分数据集
    val Array(trainingRDD, testRDD) = lpsTfIdfRDD.randomSplit(Array(0.8, 0.2))

    // TODO: c. 使用朴素贝叶斯训练模型
    val nbModel: NaiveBayesModel = NaiveBayes.train(trainingRDD)

    // 使用测试数据集进行预测
    val nbPredictAndActualRDD = testRDD.map {
      case LabeledPoint(label, features) => (nbModel.predict(features), label)
    }

    // 看看结果
    nbPredictAndActualRDD.take(10).foreach(println)

    // 多分类模型评估
    val metrics = new MulticlassMetrics(nbPredictAndActualRDD)
    println(s"使用朴素贝叶斯模型预测新闻分类，精确度ACC = ${metrics.accuracy}")

    // 混淆矩阵
    println(metrics.confusionMatrix)

    // TODO: d.模型持久化
    // nbModel.save(sc, "file:///E:/APP/BigData/api/data/models/news-model-naivebayes")

    // TODO: e. 加载模型并预测
    // 文本数据, 实际结果： b
    val title = "Fed official says weak data caused by weather, should not slow taper"
    // 特征提取
    val features = idfModel.transform(
      hashTF.transform(title.split("\\s+").map(_.toLowerCase.replace(",", "").replace(".", "")))
    )

    // load model from local fs
    val predictCategory = NaiveBayesModel
      .load(sc, "file:///E:/APP/BigData/api/data/models/news-model-naivebayes")
      .predict(features) match {
      case 0 => "b"
      case 1 => "t"
      case 2 => "e"
      case 3 => "m"
      case _ => "unknown"
    }
    println(s"加载新模型，预测结果为: ${predictCategory}")

    println("================= 下面是 word2vec ====================")
    // TODO: word2vec 将单词表示成一个向量，用于计算两个单词之间的相似度
    val inputRDD = titleCategoryRDD.map {
      case (category, newsTitle) =>
        newsTitle
          .split("\\s+")
          .map(word => word.trim.toLowerCase.replace(",", "").replace(".", ""))
          .toSeq
    }

    // 创建word2vec实例对象
    val word2vec = new Word2Vec()
    // 使用数据训练模型
    val word2VecModel = word2vec.fit(inputRDD)

    // 使用模型找出某个单词的相近词汇，取TOP20
    val sysnonys: Array[(String, Double)] = word2VecModel.findSynonyms("STOCK".toLowerCase, 10)

    for ((word, cosineSimilarity) <- sysnonys) {
      println(s"${word} 相似度: ${cosineSimilarity}")
    }

    // 为监控方便，线程休眠
    Thread.sleep(1000 * 100)
    spark.stop()
  }
}
