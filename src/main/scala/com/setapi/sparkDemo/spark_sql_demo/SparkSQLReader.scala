package com.setapi.sparkDemo.spark_sql_demo

import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/9/15
  *
  **/

object SparkSQLReader {
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
      * 读取JSON数据
      */
    val dfJson: DataFrame = sqlContext.read.json("/sparkapps/datas/src/main/resources/people.json")

    dfJson.show()

    import sqlContext.implicits._
    dfJson.select($"age").show()
    dfJson.select(dfJson("age")).show()

    dfJson.printSchema()
    // dfJson.write.json("/sparkapps/datas/src/main/resources/people_rewrite.json")


    /**
      * 读取 AVRO 数据
      * 未成功的案例
      */
    println("----> AVRO DataFrame: 未成功的案例")
    //    val avroFile = "hdfs://hnamenode:9000/sparkapps/datas/src/main/resources/users.avro"
    //    // val dfAvro = sqlContext.read.format("com.databricks.spark.avro").load(avroFile)
    //
    //    import com.databricks.spark.avro._
    //    val dfAvro2: DataFrame = sqlContext.read.avro(avroFile)
    //    dfAvro2.show()

    /**
      * 读写 CSV 数据
      */
    println("----> CSV DataFrame: ")
    val dfCsv = sqlContext.read.csv("/sparkapps/datas/src/main/resources/people.csv")
    dfCsv.show()

    val cusomSchema = StructType(
      Array(
        StructField("name2", StringType, true),
        StructField("age2", IntegerType, true),
        StructField("job2", StringType, true)
      )
    )
    val dfCsv2 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .schema(cusomSchema)
      .load("/sparkapps/datas/src/main/resources/people.csv")
    dfCsv2.printSchema()
    dfCsv2.show()


    /**
      * 读写MongoDb
      * 未成功的案例
      */
    println("----> MongoDb DataFrame: ")
    val options = Map(
      "host" -> "192.168.0.110:27017",
      "database" -> "vnpy",
      "collection" -> "db_tick_data"
    )

    val dfMongoDb = sqlContext.read.format("com.stratio.datasource.mongodb")
        .options(options).load()
    // dfMongoDb.show()




    /**
      * 程序结束
      */
    Thread.sleep(10000 * 1000)
    sc.stop()
  }
}
