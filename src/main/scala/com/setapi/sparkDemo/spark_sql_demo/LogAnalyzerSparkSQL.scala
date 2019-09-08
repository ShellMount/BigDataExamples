package com.setapi.sparkDemo.spark_sql_demo

import com.setapi.sparkDemo.logDemo.ApacheAccessLog
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/8/25
  * 使用SparkSQL实现SparkCore实现的任务
  *
  **/

object LogAnalyzerSparkSQL {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf()
      .setAppName("LogAnalyzerSparkSQL")
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
      * 处理的数据为TEXT文件，需要将数据转换为DataFrame
      * 此处采用 RDD -> DataFrame
      * 在RDD的基础上增加schema信息
      * 相当于数据库中的一张表：拥有字段的名称，类型，值
      * 它与带类的RDD的区别是，DataFrame在处理之前就
      * 知道类型
      */


    /**
      * 数据读取与业务逻辑
      */
    // val logFile = "file:///D:\\BaiduNetdiskDownload\\Spark从入门到上手实战\\0000.代码+软件+笔记+课件\\4、Spark Core（二）\\4、Spark Core（二）\\05_数据\\apache.access.log"
    val logFile = "file:///D:\\BaiduNetdiskDownload\\Spark从入门到上手实战\\0000.代码+软件+笔记+课件\\4、Spark Core（二）\\4、Spark Core（二）\\05_数据\\access_log"
    val accessLogsRDD = sc
      // 读取文件
      .textFile(logFile)
      // 过滤掉不合格数据
      .filter(log => ApacheAccessLog.isValidateLogLine(log))
      // 解析数据，将数据封装起来
      .map(log => ApacheAccessLog.parseLogLine(log))


    /**
      * RDD转为DataFrame有两种方式
      * 1，RDD[Case Class], 自动反射得到字段名称与类型
      */
    import sqlContext.implicits._
    // 隐式转换为DataFrame
    val accessLogsDF = accessLogsRDD.toDF()

    // 查看DF中的SCHEMA
    accessLogsDF.printSchema()

    // 查看DF样本数据
    accessLogsDF.show(2)

    // 注册为一张临时表，相当于HIVE中的一张表
    accessLogsDF.registerTempTable("tmp_access_log")

    // 将表中的数据进行缓存
    sqlContext.cacheTable("tmp_access_log")

    // 查询表中的数据条目数量
    sqlContext.sql("SELECT COUNT(*) AS CNT FROM tmp_access_log").show()
    /**
      * 需求1：计算content size 的最大值，最小值，平均值
      */
    val contentSizeRow: Row = sqlContext.sql(
      """
        |SELECT
        |  SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize)
        |FROM
        |  tmp_access_log
      """.stripMargin).first()
    // 输出结果
    // 下标获取： row(0)
    // 转换数据类型获取： row.getLong(1)
    // 类型Listy方式: row.get(2)
    println(s"${contentSizeRow.getLong(0) / contentSizeRow.getLong(1)}, MIN = ${contentSizeRow(2)}, MAX = ${contentSizeRow.get(3)}")

    /**
      * 需求2：返回结果的统计
      */
    val responseCodeToCount = sqlContext.sql(
      """
        |SELECT
        | responseCode, COUNT(*) AS CNT
        |FROM
        | tmp_access_log
        |GROUP BY
        | responseCode
      """.stripMargin)
      // DF 转 RDD
      .map(row => (row.getInt(0), row.getLong(1))).collect()

    println(s"ResponseCode Count : ${responseCodeToCount.mkString("[", ",", "]")}")

    /**
      * 需求3：统计IP地址的数量大于N次的
      */

    val topIPAddress = sqlContext.sql(
      """
        |SELECT
        | ipAddress, COUNT(*) AS CNT
        |FROM
        | tmp_access_log
        |GROUP BY
        | ipAddress
        |HAVING
        | CNT > 30
        |LIMIT
        | 10
      """.stripMargin)
      // DF 转 RDD
      .map(row => (row.getString(0), row.getLong(1))).collect()
    println(s"IP Address : ${topIPAddress.mkString("[", ",", "]")}")


    /**
      * 需求4： 求endpoint统计的TOPN
      */
    val topEndpoint: Array[(String, Long)] = sqlContext.sql(
      """
        |SELECT
        | endpoint, COUNT(*) AS CNT
        |FROM
        | tmp_access_log
        |GROUP BY
        | endpoint
        |ORDER BY
        | CNT DESC
        |LIMIT 5
      """.stripMargin)
      // DF 转 RDD
      .map(row => (row.getString(0), row.getLong(1))).collect()
    println(s"topEndpoint : ${topEndpoint.mkString("[", ",", "]")}")


    /**
      * 程序结束
      */
    sqlContext.uncacheTable("tmp_access_log")
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
