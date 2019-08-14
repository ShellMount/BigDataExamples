package com.setapi.sparkDemo.sparkCoreDemo

import org.apache.log4j.{Level, Logger}
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
  *
  * >>>
  * you can run app on deploy-mode of cluster :
  * ./spark-2.4.3-bin-hadoop2.7/bin/spark-submit \
  * --master spark://192.168.0.211:7077 \
  * --deploy-mode cluster \
  * --class com.setapi.sparkDemo.sparkCoreDemo.ModuleSpark  \
  * --jars \
  * hdfs://hnamenode:9000/sparkapps/AnalisysFromHiveFilesSpark-depends.jars/uasparser-0.6.1.jar,\
  * hdfs://hnamenode:9000/sparkapps/AnalisysFromHiveFilesSpark-depends.jars/hbase-common-2.2.0.jar,\
  * hdfs://hnamenode:9000/sparkapps/AnalisysFromHiveFilesSpark-depends.jars/jregex-1.2_01.jar \
  * hdfs://hnamenode:9000/sparkapps/api.jar
  *
  * >>>
  * can set resources with submit:
  * >> client deploy-mode:
  * ./bin/spark-submit --master spark://192.168.0.211:7077 \
  * --class com.setapi.sparkDemo.sparkCoreDemo.ModuleSpark \
  * --deploy-mode client \
  * --driver-memory 512M \
  * --executor-memory 1g \
  * --executor-cores 1 \
  * --total-executor-cores 2 \
  * ../api.jar
  *
  * >> cluster deploy-mode:
  * ./bin/spark-submit --master spark://192.168.0.211:7077 \
  * --class com.setapi.sparkDemo.sparkCoreDemo.ModuleSpark \
  * --deploy-mode cluster \
  * --driver-memory 512M \
  * --driver-cores 2 \
  * --executor-memory 1g \
  * --executor-cores 1 \
  * --total-executor-cores 2 \
  * ../api.jar
  *
  * >>> 特别的设置优化点
  * 运行在standalone模式下时，work目录的设置： ${SPARK_HOME}/conf/spark-env.sh
  * SPARK_WORKER_DIR     ： 默认在 ${SPARK_HOME}/work
  * SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true \
  *   -Dspark.worker.cleanup.interval=1800 \
  *   -Dspark.worker.cleanup.appDataTt=7*24*3600"
  *
  * >>> 运行日志保留: 关闭APP后，在WEBUI：18080中再次打开APP运行JOB日志
  * 默认配置文件中设置： ${SPARK_HOME}/spark-defaults.conf
  * SPARK_HISTORY_OPTS
  * spark.eventLog.enabled  true
  * spark.eventLog.dir      hdfs://hnamenode:9000/datas/spark-running-logs/
  * spark.eventLog.compress true
  * 也可在SparkConf中set
  *
  *
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
      // 开发环境设置：-Dspark.master=spark://192.168.0.211:7077
      .setMaster("local[2]")
      // 设置记录此应用的事件EventLog
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.compress", "true")
      .set("spark.eventLog.dir", "hdfs://hnamenode:9000/datas/spark-running-logs/ModuleSpark")

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

    // 多次使用的RDD放到内存
    wordCountRDD.cache()

    // 对统计出的词频排序
    val sortedRDD = wordCountRDD.map(tuple => tuple.swap).sortByKey(false)
    val sortedRDD2 = wordCountRDD.sortBy(_._2, false)


    /**
      * 结果输出
      *
      * RDD#take 和 RDD#TOP 将数据以数据的形式返回给Driver,
      * 然后在Driver上显示
      *
      * 对RDD直接的操作，将在Executor中执行并显示。
      */
    // 本行结果显示在Executors，不会在Driver提交的控制台显示结果
    wordCountRDD.foreach(println)

    println("--------------SORT-----------------")
    sortedRDD.take(10).foreach(tuple => println(tuple._1 + "-->" + tuple._2))

    println("--------------TOP-----------------")
    println(wordCountRDD.map(_.swap).top(10).foreach(println))
    // logger.warn("分析结束。")
    /**
      * 关闭资源
      */
    // 解除缓存
    wordCountRDD.unpersist()

    // 为了开发测试时监控页面能够看到Job，线程暂停
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
