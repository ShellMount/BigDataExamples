package com.setapi.sparkDemo.spark_sql_demo

import java.lang.ClassNotFoundException
import java.sql.{DriverManager, ResultSet}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
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

object SparkReadHiveTableSqlJdbc {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val driverName = "org.apache.hive.jdbc.HiveDriver"

    try {
      Class.forName(driverName)
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
      System.exit(1)
    }

    // 连接HIVE
    val conn = DriverManager.getConnection("jdbc:hive2://hdatanode1:10000/default", "", "")
    val stmt = conn.createStatement()

    // 执行语句
    val sqlTextShowDatabases =
      """
        |SHOW DATABASES
      """.stripMargin
    val databasesSet: ResultSet = stmt.executeQuery(sqlTextShowDatabases)
    println(s"----> ${sqlTextShowDatabases} :")
    while (databasesSet.next()) {
      println(databasesSet.getString(1))
    }

    // 查询语句
    val sqlSelect =
      """
        |SELECT * FROM hiveonhdfs.dept
      """.stripMargin
    val res: ResultSet = stmt.executeQuery(sqlSelect)
    println(s"----> ${sqlSelect} :")
    while (res.next()) {
      println(s"${res.getInt(1)},  ${res.getString(2)},  ${res.getString(3)}")
    }


  }
}
