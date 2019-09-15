package com.setapi.sparkDemo.spark_sql_demo

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
  * 程序调试未成功
  **/

object SparkReadMongoDbTableSQL {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf

    val sparkSession = SparkSession.builder()
      .appName("SparkReadMongoDbTableSQL")
      .master("local[2]")
      .getOrCreate()

    val builder = MongodbConfigBuilder(Map(
      Host -> List("192.168.0.110:27017"),
      Database -> "vnpy",
      Collection ->"db_tick_data",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal")
    )
    val readConfig = builder.build()
    val mongoRDD = sparkSession.sqlContext.fromMongoDB(readConfig)
    mongoRDD.createTempView("db_tick_data")
    val dataFrame = sparkSession.sql("SELECT * FROM db_tick_data")
    dataFrame.show


    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)

  }
}
