package com.setapi.sparkDemo.spark_core_demo

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.setapi.bigdata.java.common.EventLogConstants
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
  * -Dspark.worker.cleanup.interval=1800 \
  * -Dspark.worker.cleanup.appDataTt=7*24*3600"
  *
  * >>> 运行日志保留: 关闭APP后，在WEBUI：18080中再次打开APP运行JOB日志
  * 默认配置文件中设置： ${SPARK_HOME}/spark-defaults.conf
  * SPARK_HISTORY_OPTS
  * spark.eventLog.enabled  true
  * spark.eventLog.dir      hdfs://hnamenode:9000/datas/spark-running-logs/
  * spark.eventLog.compress true
  *
  * OR：
  * ${SPARK_HOME}/conf/spark-env.sh
  * export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 \
  * -Dspark.history.retainedApplications=3 \
  * -Dspark.history.fs.logDirectory=hdfs:/hnamenode:9000/datas/spark-running-logs
  * -Dspark.history.fs.cleaner.enabled=true \
  * "
  *
  * 也可在SparkConf中set
  *
  *
  *
  **/


object RDDOperationSpark {
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
      .setAppName("RDDOperationSpark")
    // 本地开发环境设置为local mode, 2线程
    // 实际部署的时候，通过提交命令行进行设置
    .setMaster("local[2]")

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
    val linesRDD2 = sc.textFile("/bigdata/datas/usereventlogs/2015-12-20/20151220.log", 4)
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
        val eventAlias = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME)

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
    val pvCountRDD = filteredRDD
      .map {
        case (date, url, uuid) => (date, 1)
      }
      .reduceByKey(_ + _)


    // 统计每日UV
    val uvCountRDD: RDD[(String, Int)] = filteredRDD
      .map {
        case (date, url, uuid) => (date, uuid)
      }
      // 去重
      .distinct()
      .map {
        case (date, uuid) => (date, 1)
      }
      .reduceByKey(_ + _)

    /**
      * RDD常见操作
      */

    // TODO: 常用的RDD 操作
    println("================== joinRDD ==================")
    val joinRDD: RDD[(String, (Int, Int))] = pvCountRDD.join(uvCountRDD)
    joinRDD.foreach {
      case (date, (pv, uv)) => println(s"date: ${date}, PV = ${pv}, UV = ${uv}")
    }


    println("================== unionRDD ==================")
    val unionRDD = pvCountRDD.union(uvCountRDD).union(uvCountRDD).union(uvCountRDD).union(uvCountRDD)
    unionRDD.foreach {
      case (date, count) => println(s"date: ${date}, count = ${count}")
    }

    println("================== mapPartitions ==================")
    // 参见 parseEventLogsRDD 的计算

    val unionRDD2 = unionRDD.map{tuple => println("---- ")}.count()


    val unionRDD3 = unionRDD.mapPartitions(iter => {
      println("====")
      iter.map(item => {})
    }).count()

    println("================== coalesce/repartition ==================")

    /**
      * 增加RDD分区：情况不多，考虑并行度，一般在读取数据时，指定好分区数
      * 减少RDD分区：一个分区对应一个TASK进行处理
      * 如果分区中的数据较少，或者没有，则会效率降低
      * 通常在将结果保存至外部时，如RDBMS/HDFS等。
      */
    unionRDD.coalesce(1)
    unionRDD.repartition(20)

    println("================== foreach/foreachPartitions ==================")
    // 对不为空的RDD进行处理
    if (!unionRDD.isEmpty()) {
      unionRDD.foreachPartition(iter => {
        // 建立连接
        // val conn = ...
        iter.map(item => {
          // 插入每一条数据
          // 或组织为一个列表，
          // 然后在关闭链接之前统一插入
        })
        // 关闭连接
        // conn.close()
      })
    }

    println("================== countByKey ==================")
    val pvCountByKeyRDD2: collection.Map[String, Long] = filteredRDD
      .map {
        case (date, url, uuid) => (date, 1)
      }
      .countByKey()


    println("================== combineByKey ==================")



    println("================== aggregateByKey ==================")



    println("================== foldByKey ==================")



    println("================== xxx ==================")


    /**
      * 结果输出
      *
      * RDD#take 和 RDD#TOP 将数据以数据的形式返回给Driver,
      * 然后在Driver上显示
      *
      * 对RDD直接的操作，将在Executor中执行并显示。
      */
    println
    println
    println
    println("================== 结果输出区 ==================")
    println(s"pvCount = ${pvCountRDD}")
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
