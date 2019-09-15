package com.setapi.sparkDemo.spark_sql_demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/9/15
  *
  **/

object SparkSQLUDAF {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf()
      .setAppName("SparkSQLReader")
      .setMaster("local[2]")

    // 创建 SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)

    /**
      * 使用SparkSQL进行数据分析，需要创建SQLContext来讯取数据
      * 以转换为DataFrame
      * 它是SparkSQL的入口
      */
    val sqlContext = SQLContext.getOrCreate(sc)


    /**
      * UDF:
      * 创建完SQLContext后就进行自定义函数的定义与注册
      */
    //
    sqlContext.udf.register(
      "addAge",
      (age: Int) => {age + 100}
    )

    /**
      * UDAF 定义：聚合的值
      * 获取年龄最大的人
      */


    /**
      * 读取JSON数据
      */
    val dfJson: DataFrame = sqlContext.read.json("/sparkapps/datas/src/main/resources/people.json")

    dfJson.show()

    dfJson.registerTempTable("emp")

    sqlContext.sql("SELECT avg(age) FROM emp").show()

    // 保留0位小数
    sqlContext.sql("SELECT round(avg(age), 0) FROM emp").show()

    // 增加年龄
    sqlContext.sql("SELECT age, addAge(age) FROM emp").show()



    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
