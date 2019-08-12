package com.setapi.sparkDemo.sparkCoreDemo

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/8/12
  *
  **/

object WordCount {
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sc = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[2]")

    val ssc = new SparkContext(sc)

    val linesRDD = ssc.textFile("/bigdata/datas/usereventlogs/2015-12-20/20151220.log")

    logger.warn("RDD中每个元素的数据类型是Array[T] : " + linesRDD.take(1).getClass)


    logger.warn(s"RDD文件有 ${linesRDD.count()} 行")
    val words = linesRDD.flatMap(_.split("\\^"))
    val count = words.map(_.trim).filter(!_.equals("")).count()
    logger.warn(s"文件中共有单词数量: ${count}")

    logger.warn(s"词频统计: ")
    val wordCount: RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_+_).sortBy(_._2, false)
    wordCount.take(10).foreach(println)

    logger.warn("collect中保存 : 实际环境中很有可能占用很大的内存")
    wordCount.collect.take(5).foreach(println)

    val path = new Path("/datas/wordcount.txt")
    val fs = FileSystem.get(ssc.hadoopConfiguration)
    if(!fs.exists(path)) wordCount.saveAsTextFile("/datas/wordcount.txt")

    Thread.sleep(10000*1000)

  }
}
