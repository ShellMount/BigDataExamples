package com.setapi.sparkDemo.sparkCoreDemo

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.setapi.bigdata.java.common.EventLogConstants
import com.setapi.bigdata.java.common.EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME
import com.setapi.bigdata.java.util.LogParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/8/13
  *
  * Spark Application 编程模板
  *
  * run on cluster:
  * ./bin/spark-submit --master spark://192.168.0.211:7077 \
  * --class com.setapi.sparkDemo.sparkCoreDemo.ModuleSpark ../api.jar
  **/

object AnalysisFromHiveFilesSpark {
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
      .setAppName("AnalysisFromHiveFilesSpark")
      // 本地开发环境设置为local mode, 2线程
      // 实际部署的时候，通过提交命令行进行设置
      // .setMaster("local[2]")

    // 创建SparkContext上下文对象
    val sc = SparkContext.getOrCreate(conf)

    /**
      * 读取数据
      * HIVE中的数据在文件中的存储以“\t”分隔
      */
    // 从HDFS读取数据，需要 core-site.xml / hdfs-site.xml 配置文件
    // HIVE仓库中的数据
    val linesRDD = sc.textFile("/user/hive/warehouse/hiveonhdfs.db/emp_partition/month=201907/day=13")
    // HDFS上的数据，由于数据格式的不同，本次分析，以此为准
    val linesRDD2 = sc.textFile("/bigdata/datas/usereventlogs/2015-12-20/20151220.log")
    // HBase仓库中的数据, 不能直接以文件的方式读取
    val linesRDD3 = sc.textFile("/hbase/default/event_logs_20151220/02e884109a5af31dce6faf1b3cd2d1b7")
    println(s"Count = ${linesRDD2.count()}, the first=${linesRDD2.first()}")

    /**
      * 处理数据
      * 统计每日的PV/UV
      */
    // 有可能需要过滤数据
    val parseEventLogsRDD = linesRDD2.mapPartitions(iter => {
      iter.map(log => {
        // 调用工具解析得到 Map 集合
        val logInfo: util.Map[String, String] = new LogParser().handleLogParser(log)

        // 获取事件的类型
        val eventAlias = logInfo.get(LOG_COLUMN_NAME_EVENT_NAME)

        (eventAlias, logInfo)
      })
    })

    val filteredRDD = parseEventLogsRDD
      .filter {
        case (eventAlias, logInfo) =>
          logInfo.size() != 0 && logInfo.getOrDefault(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL, "").trim.length > 0
      }
      .mapPartitions(iter => {
        iter.map { case (eventAlias, logInfo) =>
          val timestamp2Long = (logInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME).toFloat * 1000).toLong
          (new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp2Long))
            ,
            logInfo.get(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL),
            logInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID))
        }
      })

    // 放到内存中: 需要Executor内存较大
    filteredRDD.cache()

    // TODO:统计每日的PV: 没有按照时间分组，算法有误。
    val pvCount = filteredRDD.count()



    // 统计每日UV
    val uvCountRDD: RDD[(String, Int)] = filteredRDD
      .map {
        case (date, url, uuid) => (date, uuid)
      }
      // 去重
      .distinct()
      .map{
        case(date, uuid) => (date, 1)
      }
      .reduceByKey(_ + _)


    /**
      * 结果输出
      *
      * RDD#take 和 RDD#TOP 将数据以数据的形式返回给Driver,
      * 然后在Driver上显示
      *
      * 对RDD直接的操作，将在Executor中执行并显示。
      */
    println(s"pvCount = ${pvCount}")
    println(s"uvCount = ")
    // 下面的打印信息将显示在 Executor 的日志标准输出中
    uvCountRDD.foreach(println)

    /**
      * 关闭资源
      */
    // 释放缓存
    filteredRDD.unpersist()

    // 为了开发测试时监控页面能够看到Job，线程暂停
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
