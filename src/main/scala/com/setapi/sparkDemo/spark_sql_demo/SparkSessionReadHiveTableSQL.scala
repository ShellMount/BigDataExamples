package com.setapi.sparkDemo.spark_sql_demo

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  *
  * Created by ShellMount on 2019/8/25
  * 使用SparkSQL实现SparkCore实现的任务
  *
  * 好像这个并不是必须的，对于APP来说有hive-site.xml后可以自行定位
  * /opt/spark/spark-2.4.3-bin-hadoop2.7/sbin/start-thriftserver.sh
  * --hiveconf hive.server2.thrift.port=10000
  * --hiveconf hive.server2.thrift.bind.host=hdatanode1
  * --hiveconf hive.server2.webui.port=10002 \
  * --hiveconf hive.server2.webui.host=hdatanode1 \
  * --conf spark.sql.shuffle.partitions=5 \
  * --master local[2]
  *
  * client mode:
  * !!: it must be the version of spark client beeline
  * to connect the thrift server
  **/

object SparkSessionReadHiveTableSQL extends Throwable {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    // 创建 SparkSession
    val spark = SparkSession
      .builder()
      .appName("SparkReadHiveTableSQLReadJdbc")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///E:\\APP\\BigData\\api\\spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    /**
      * 在Spark中使用SQL读取Hive表信息
      * 1, 需要添加hive-site.xml
      * 2, 需要添加mysql-connect库
      * 3, 在yarn cluster上运行时
      * 需要使用
      * --jars添加依赖
      * --file添加配置
      */

    // 在集群环境中命令行下可以直接这么使用，
    // 但开发环境中，未能成功读取到数据库名称
    // ./spark-shell 中使用正常
    spark.sql("show databases").show()
    // 找不到下面的表
    spark.sql("use hiveonhdfs")
    spark.sql("show tables").show()

    println("---->")
    spark.sql("SELECT * FROM hiveonhdfs.emp_partition").show()

    println("---->")
    spark.read.table("hiveonhdfs.dept").show()

    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)
  }
}
