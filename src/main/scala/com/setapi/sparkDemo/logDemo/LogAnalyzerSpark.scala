package com.setapi.sparkDemo.logDemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/8/25
  *
  **/

object LogAnalyzerSpark {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf()
      .setAppName("LogAnalyzerSpark")
      .setMaster("local[2]")

    // 创建 SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)

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

    // 缓存数据
    accessLogsRDD.persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Count: ${accessLogsRDD.count()} \n${accessLogsRDD.first()}")


    /**
      * 需求1：计算content size 的最大值，最小值，平均值
      */
    val contentSizeRDD: RDD[Long] = accessLogsRDD.map(_.contentSize)
    contentSizeRDD.cache()

    // 计算
    val avgContentSize = contentSizeRDD.reduce(_ + _) / contentSizeRDD.count()
    val minContentSize = contentSizeRDD.min()
    val maxContentSize = contentSizeRDD.max()

    // 释放内存
    contentSizeRDD.unpersist()

    println(s"avgContentSize = ${avgContentSize}, minContentSize = ${minContentSize}, maxContentSize = ${maxContentSize}")

    /**
      * 需求2：返回结果的统计
      */
    val responseCodeToCount = accessLogsRDD
      // 转换字段
      .map(log => (log.resposeCode, 1))
      // 聚合统计
      .reduceByKey(_ + _)
      // 由于Response Code的数量不多可以直接返回
      .collect()

    println(s"Response Code Count : ${responseCodeToCount.mkString("[", ", ", "]")}")

    /**
      * 需求3：统计IP地址的数量大于N次的
      */
    val ipAddress: Array[(String, Int)] = accessLogsRDD
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      // 访问网站超过20次的IP
      .filter(_._2 > 0)
      .take(20)

    println(s"IP Address Access Count: ${ipAddress.mkString("[", ", ", "]")}")


    /**
      * 需求4： 求endpoint统计的TOPN
      */
    val topEndpointRDD = accessLogsRDD
      .map(log => (log.endpoint, 1))
      .reduceByKey(_ + _)
      // .sortBy(_._2, false)
      // .take(10)
      // 使用TOP, 能够提升速度
      .top(5)(OrderingUtils.SecondValueOrdering)

    println(s"endpoint sorted: ${topEndpointRDD.mkString("[", ", ", "]")}")


    /**
      * 程序结束
      */
    accessLogsRDD.unpersist()
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
