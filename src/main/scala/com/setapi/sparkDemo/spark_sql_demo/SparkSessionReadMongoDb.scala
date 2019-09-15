package com.setapi.sparkDemo.spark_sql_demo

import com.mongodb.spark.MongoSpark
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  *
  * Created by ShellMount on 2019/8/25
  * 使用SparkSQL实现 MongoDb 读写
  *
  * https://blog.csdn.net/qq_33689414/article/details/83421766
  * https://docs.mongodb.com/spark-connector/current/scala-api/
  *
  * 程序调试未成功
  **/

object SparkSessionReadMongoDb {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // 创建 SparkSession
    val sparkSession = SparkSession.builder()
      .appName("SparkSessionReadMongoDb")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "file:///E:\\APP\\BigData\\api\\spark-warehouse")
      .config("spark.mongodb.input.uri", "mongodb://192.168.0.110:27017/vnpy.db_tick_data")
      .getOrCreate()

    val df = MongoSpark.load(sparkSession)
    df.show


    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)

  }
}
