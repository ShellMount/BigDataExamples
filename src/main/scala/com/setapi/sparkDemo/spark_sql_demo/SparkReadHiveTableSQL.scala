package com.setapi.sparkDemo.spark_sql_demo

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/8/25
  * 使用SparkSQL实现SparkCore实现的任务
  *
  * /opt/spark/spark-2.4.3-bin-hadoop2.7/sbin/start-thriftserver.sh
  * --hiveconf hive.server2.thrift.port=10000
  * --hiveconf hive.server2.thrift.bind.host=hdatanode1
  * --conf spark.sql.shuffle.partitions=5 \
  * --master local[2]
  *
  * client mode:
  * !!: it must be the version of spark client beeline
  * to connect the thrift server
  **/

object SparkReadHiveTableSQL extends Throwable {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf()
      .setAppName("SparkReadHiveTableSQL")
      .setMaster("local[2]")


    // 创建 SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)

    /**
      * 使用SparkSQL进行数据分析，需要创建SQLContext来讯取数据
      * 以转换为DataFrame
      * 它是SparkSQL的入口
      */
    val sqlContext = SQLContext.getOrCreate(sc)
    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)


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
    sqlContext.sql("show databases").show()
    // 找不到下面的表
    //sqlContext.sql("SELECT * FROM hiveonhdfs.dept").show()

    //sqlContext.sql("use hiveonhdfs")
    //    sqlContext.sql("SELECT * FROM emp_partition").show()



    /**
      * 从JDBC中读取DataFrame数据
      *
      * 未成功的连JDBC连接
      */
//    val properties = new Properties()
//    properties.put("user", "root")
//    properties.put("password", "birdhome")
//
//
//    val hiveContext = new HiveContext(sc)
//    hiveContext.sql("SELECT * FROM hiveonhdfs.dept").show()
//
//    val df: DataFrame = sqlContext.read.jdbc("jdbc:hive2://hdatanode1:10000/hiveonhdfs", "dept", properties)
//    val df2: DataFrame = hiveContext.read.format("jdbc")
//      .option("url", "jdbc:hive2://hdatanode1:10000/hiveonhdfs")
//      .option("dbtable", "dept")
//      .option("user", "root")
//      .option("password", "birdhome")
//      .load()
//
//    println("通过JDBC读取出来的HIVE TABLE DataFrame:")
//    df2.show()

    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
