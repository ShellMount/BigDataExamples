package com.setapi.sparkDemo.spark_sql_demo

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/9/15
  *
  **/

object SparkSQLUDAF2 {
  // 日志设置
  // Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf()
      .setAppName("SparkSQLUDAF2")
      .setMaster("local[2]")

    // 创建 SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)

    /**
      * 使用SparkSQL进行数据分析，需要创建SQLContext来讯取数据
      * 以转换为DataFrame
      * 它是SparkSQL的入口
      */
    val sqlContext: SQLContext = SQLContext.getOrCreate(sc)


    /**
      * UDF:
      * 创建完SQLContext后就进行自定义函数的定义与注册
      */
    //
    sqlContext.udf.register(
      "addAge",
      (age: Int) => {
        age + 100
      }
    )

    /**
      * UDAF 定义：聚合的值
      * 获取年龄最大的人
      */


    /**
      * 读取JSON数据
      */
    //    val dfJson: DataFrame = sqlContext.read.json("/sparkapps/datas/src/main/resources/people.json")
    //
    //    dfJson.show()
    //
    //    dfJson.registerTempTable("emp")
    //
    //    sqlContext.sql("SELECT avg(age) FROM emp").show()
    //
    //    // 保留0位小数
    //    sqlContext.sql("SELECT round(avg(age), 0) FROM emp").show()
    //
    //    // 增加年龄
    //    sqlContext.sql("SELECT age, addAge(age) FROM emp").show()

    /**
      * UDAF函数
      */
    createUdf(sqlContext)

    val sqlText =
      """
        |SELECT
        |   SPLIT(v.order_recode, ',')[0] AS content_id,
        |   SPLIT(v.order_recode, ',')[1] AS goods_price,
        |   SPLIT(v.order_recode, ',')[2] AS goods_quantity
        |FROM (
        |   SELECT
        |       zip2(
        |          -- content_id, goods_price, goods_quantity
        |          --'457037306,368578906,452570101,452570106,457171403,455532105,455003602,458172310,451717106,212395210-13.99,13.99,13.99,13.99,19.99,10.99,6.52,8.470000000000001,13.99,13.99-1,1,1,1,1,1,1,1,1,1'
        |          '457037306-8.470000000000001'
        |      ) AS zip_column
        |   ) t
        |   LATERAL VIEW EXPLODE(SPLIT(zip_column, '->')) v AS order_recode
      """.stripMargin
    sqlContext.sql(sqlText).show(truncate=false)

    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)
    sc.stop()
  }

  def createUdf(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(
      "zip2",
      (input: String) => {
        val Array(content_id, goods_price) = input.split("-")
        content_id.split(",")
          .zip(goods_price.split(","))
          .map{case (id, price) => s"${id},${price}"}
          .mkString("->")
      }
    )

    sqlContext.udf.register(
      "zip3",
      (input: String) => {
        val Array(content_id, goods_price, goods_quantity) = input.split("-")
        content_id.split(",")
          .zip(goods_price.split(","))
          .map{case (id, price) => s"${id},${price}"}
          .zip(goods_quantity.split(","))
          .map{case (id_price, quantity) => s"${id_price},${quantity}"}
          .mkString("->")
      }
    )
  }

}
