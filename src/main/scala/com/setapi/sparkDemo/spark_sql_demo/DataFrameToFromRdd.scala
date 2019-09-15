package com.setapi.sparkDemo.spark_sql_demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/9/15
  *
  * RDD 与 DataFrame 互相转换的几种方式
  *
  **/



object DataFrameToFromRdd {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf()
      .setAppName("SparkSQLReader")
      .setMaster("local[2]")

    // 创建 SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)

    /**
      * 使用SparkSQL进行数据分析，需要创建SQLContext来讯取数据
      * 以转换为DataFrame
      * 它是SparkSQL的入口
      */
    val sqlContext = SQLContext.getOrCreate(sc)

    val rdd = sc.textFile("/sparkapps/datas/src/main/resources/people.txt")

    /**
      * RDD[String] -> RDD[ROW]
      */
    // 生成 RD[Row]
    import org.apache.spark.sql.Row
    val rowRDD = rdd.map(line => {
      val splited = line.split(",")
      Row(splited(0), splited(1).trim.toInt)
    })

    // 定义Schema
    val customSchema = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )

    // 创建DF
    val df = sqlContext.createDataFrame(rowRDD, customSchema)

    df.show()


    /**
      * 也可以使用 case class 反射的方法，更常用
      */

    val personRdd: RDD[Person] = rdd.map(line => {
      val splited = line.split(",")
      Person(splited(0), splited(1).trim.toInt)
    })

    import sqlContext.implicits._
    val personDF = personRdd.toDF()
    personDF.show()
  }
}
