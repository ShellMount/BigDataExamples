package com.setapi.sparkDemo.spark_sql_demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/8/25
  * 使用SparkSQL实现SparkCore实现的任务
  *
  **/

object SparkSQLGetJson {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf()
      .setAppName("SparkSQLGetJson")
      .setMaster("local[2]")

    // 创建 SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)

    /**
      * 使用SparkSQL进行数据分析，需要创建SQLContext来讯取数据
      * 以转换为DataFrame
      * 它是SparkSQL的入口
      */
    val sqlContext = SQLContext.getOrCreate(sc)

    /**
      * 从JSON字段中取值
      */
    val sqlText =
      raw"""
        |SELECT
        |get_json_object('{"sort": "value_of_sort"}', '$$.sort')
      """.stripMargin

    sqlContext.sql(sqlText).show()

    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
