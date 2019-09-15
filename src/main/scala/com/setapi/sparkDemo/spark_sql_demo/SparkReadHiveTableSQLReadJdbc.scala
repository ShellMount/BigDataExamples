package com.setapi.sparkDemo.spark_sql_demo

import java.util.Properties

import org.apache.log4j.{Level, Logger}
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

object SparkReadHiveTableSQLReadJdbc {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf

//    val sparkConf = new SparkConf()
//      .setAppName("SparkReadHiveTableSQLReadJdbc")
//      .setMaster("local[2]")
//
//    // 创建 SparkContext
//    val sc = SparkContext.getOrCreate(sparkConf)
//
//    /**
//      * 使用SparkSQL进行数据分析，需要创建SQLContext来讯取数据
//      * 以转换为DataFrame
//      * 它是SparkSQL的入口
//      */
//    val sqlContext = SQLContext.getOrCreate(sc)


    val spark = SparkSession
      .builder()
      .appName("SparkReadHiveTableSQLReadJdbc")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    /**
      * 从JDBC中读取DataFrame数据
      *
      * 未成功的连JDBC连接
      */
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "birdhome")
    properties.put("driver", "org.apache.hive.jdbc.HiveDriver")

    val df: DataFrame = spark.read.jdbc("jdbc:hive2://hdatanode1:10000/hiveonhdfs", "dept", properties)
    df.createOrReplaceTempView("dept")


    println("通过JDBC读取出来的HIVE TABLE DataFrame:")
    println(df.printSchema())

    //spark.sql("SELECT * FROM dept").show()
    df.select("dept.dname").show(1)


    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)
  }
}
