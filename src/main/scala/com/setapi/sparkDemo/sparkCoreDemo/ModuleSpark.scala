package com.setapi.sparkDemo.sparkCoreDemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/8/13
  *
  * Spark Application 编程模板
  *
  **/

object ModuleSpark {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  /**
    * 如果Spark APP运行在本地：DriverProgram
    * JVM Process
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    /**
      * SparkContext 对象, Spark 分析的入口
      */
    val conf = new SparkConf()
      .setAppName("ModuleSpark")
      // 本地开发环境设置为local mode, 2线程
      // 实际部署的时候，通过提交命令行进行设置
      .setMaster("local[2]")

    // 创建SparkContext上下文对象
    val sc = SparkContext.getOrCreate(conf)

    /**
      * 读取数据
      */
    // 从HDFS读取数据，需要 core-site.xml / hdfs-site.xml 配置文件
    val linesRDD = sc.textFile("/bigdata/datas/usereventlogs/2015-12-20/20151220.log")


    /**
      * 处理数据
      */
    val tupleRDD = linesRDD.flatMap(line => line.split("\\^").filterNot(_.trim.equals("")).map(word => (word, 1)))

    val wordCountRDD = tupleRDD.reduceByKey(_ + _)

    // 对统计出的词频排序
    val sortedRDD = wordCountRDD.map(tuple => tuple.swap).sortByKey(false)
    val sortedRDD2 = wordCountRDD.sortBy(_._2, false)


    /**
      * 结果输出
      */
    println("--------------SORT-----------------")
    sortedRDD.take(10).foreach(tuple => println(tuple._1 + "-->" + tuple._2))

    println("--------------TOP-----------------")
    println(wordCountRDD.map(_.swap).top(10).foreach(println))
    // logger.warn("分析结束。")
    /**
      * 关闭资源
      */
    // 为了开发测试时监控页面能够看到Job，线程暂停
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
