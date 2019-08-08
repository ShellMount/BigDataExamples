package com.setapi.etlstreaming

import java.util

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}


/**
  *
  * Created by ShellMount on 2019/8/8
  *
  * SparkStreaming 采用 Direct 方式从Kafka Topic中读JSON数据
  *
  * 按照TAG TYPE插入到不同的HBASE表中
  * 使用RDD自定义分区器实现
  *
  * TODO: 为了简单起见， orderType 类型有四种，所以
  * HBase 数据库中有四张表，每张表的列簇为info, 仅有
  * 一个value创建表的时候，设置预分区，表的ROWKEY就是
  * orderId(随机生成)
  * 表名：
  * etl_alipay, etl_weixin, etl_car, etl_other
  *
  * 预分区：
  * 2e4348f1-60df-4a01-a2c3-6bf42d48e17b
  * 6e3a0318-1e39-4c9b-95c0-1447c66f54de
  * 9332f270-03b8-405f-9681-2b17c6f02dde
  * cc94fa3d-5dda-4f50-bced-c5e428621bc3
  * f988e06c-c365-416d-bb5b-d1b650905113
  *
  * HBase shell:
  * -创建命名空间
  *   create_namespace 'etlns'
  * -创建表，指定预计分区，使用snappy压缩
  *   create 'etlns:etl_alipay', {NAME => 'info', COMPRESSION => 'SNAPPY'},  SPLITS => ['2e4348f1', '6e3a0318', '9332f270', 'cc94fa3d']
  *   create 'etlns:etl_weixin', {NAME => 'info', COMPRESSION => 'SNAPPY'},  SPLITS => ['2e4348f1', '6e3a0318', '9332f270', 'cc94fa3d']
  *   create 'etlns:etl_car', {NAME => 'info', COMPRESSION => 'SNAPPY'},  SPLITS => ['2e4348f1', '6e3a0318', '9332f270', 'cc94fa3d']
  *   create 'etlns:etl_other', {NAME => 'info', COMPRESSION => 'SNAPPY'},  SPLITS => ['2e4348f1', '6e3a0318', '9332f270', 'cc94fa3d']
  *
  *
  **/

object EtlKafkaToHBase {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  // HBASE连接
  val conf = HBaseConfiguration.create()
  val conn = ConnectionFactory.createConnection(conf)
  // 定义连接HBase CLuster
  val admin = conn.getAdmin.asInstanceOf[HBaseAdmin] //父类转子类

  // HBase 表中的列簇名称
  val HBASE_ETL_TABLE_FAMILY_TYPES = Bytes.toBytes("info")

  //  HBase 表中的列名称
  val HBASE_ETL_TABLE_COLUMN_TEYPS = Bytes.toBytes("value")

  /**
    * Spark Application 入口，创建SparkContext实例对象
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    /**
      * 构建一个 StreamingContext 流式上下文实例对象，用于读取Kafka数据
      */
    // 创建SparkConf实例对象，设置应用配置信息
    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("EtlKafkaToHBase")
    // 构建 StreamingContext实例，设置批次处理时间间隔
    val sc = new StreamingContext(sparkConf, Seconds(2))

    /**
      * 从流式数据源实时读数据
      * 进行ETL并存储到HBase
      */
    // KAFKA配置信息
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> EtlConstant.METADATA_BROKER_LIST,
      "auto.offset.reset" -> "largest"
    )
    // TOPIC
    val topics = Set("MYFIRSTTOPIC")
    // 采用 Direct 方式从 Kafka Topic中读取数据
    val kafkaDStream: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        sc,
        kafkaParams,
        topics
      )
    logger.warn("显示三条KAFKA读取到的数据")
    kafkaDStream.print(3)

    /**
      * DStream.foreachRDD 为处理数据时使用, 返回Unit
      * DStream.transform 为转换数据时使用，返回DStream
      */
    kafkaDStream.foreachRDD((rdd, time) => {
      // 针对 RDD 每个分区操作，解析Message JSON数据，获取定单类型
      val orderRDD: RDD[(String, String)] = rdd.mapPartitions(iter => {
        iter.map {
          case (_, msg) => {
            // 解析获取Order数据
            val orderType = JSON.parseObject(msg).getString("orderType")
            // 返回
            (orderType, msg)
          }
        }
      })

      // 对orderRDD 重分区，按orderType类型的分区
      val partitionerRDD = orderRDD.partitionBy(new OrderTypePartitioner)

      // 测试输出：
      partitionerRDD.foreachPartition(iter => {
        // 由于每个RDD的分区数据被一个Task处理，每个Task 有一个 TaskContext，可获取分区ID
        val partitionId = TaskContext.getPartitionId()
        logger.warn(s"--->按分区显示数据partitionId = ${partitionId}")
        iter.take(3).foreach(println)
      })
    })

    /**
      * 上面的另一个写法：
      */
      // 解析DStream数据
    val orderTypeDStream = kafkaDStream.transform(rdd => {
      rdd.mapPartitions(_.map {
        case (_, msg) => {
          // 解析获取Order数据
          val orderType = JSON.parseObject(msg).getString("orderType")
          // 返回
          (orderType, msg)
        }
      })
    })

    // 将数据重分区，并插入到HBase中
    orderTypeDStream.foreachRDD((rdd, time) =>
      rdd
        .partitionBy(new OrderTypePartitioner)
        .foreachPartition(iter => {
          // 由于每个RDD的分区数据被一个Task处理，每个Task 有一个 TaskContext，可获取分区ID
          val partitionId = TaskContext.getPartitionId()
          logger.warn(s"===>按分区显示数据partitionId = ${partitionId}")
          iter.take(3).foreach(println)

          partitionId match {
            case 0 => {insertIntoHBase("etlns:etl_alipay", iter)}
            case 1 => {insertIntoHBase("etlns:etl_weixin", iter)}
            case 2 => {insertIntoHBase("etlns:etl_card", iter)}
            case _ => {insertIntoHBase("etlns:etl_other", iter)}
          }
        })
    )


    /**
      * 启动流式应用
      */
    sc.start()
    sc.awaitTermination()
    sc.stop(stopSparkContext = true, stopGracefully = true)
  }


  /**
    * 将JSON格式的数据插入到HBase表中
    * 其中表的ROWKEY为订单的ID：orderId
    * @param tableName
    *                  表名
    * @param iterable
    *                 迭代器，二元组中Value为存储的数据
    *                 在HBase中为info:value
    */
  def insertIntoHBase(tableName: String, iterable: Iterator[(String, String)]): Unit = {

    // 获取HBase连接, 程序最上面已经处理

    // 获取表的句柄
    val table: HTable = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

    // 迭代器插入
    val puts = new util.ArrayList[Put]()
    iterable.foreach{
      case(_, jsonValue) => {
        // 解析JSON格式数据，获取ROWKEY
        val rowKey = JSON.parseObject(jsonValue).getString("orderId")

        // 创建Put对象
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(
          HBASE_ETL_TABLE_FAMILY_TYPES,
          HBASE_ETL_TABLE_COLUMN_TEYPS,
          // 同时插入多列
          Bytes.toBytes(jsonValue))

        // 将put添加到列表中
        puts.add(put)
      }
    }

    // 批量插入到HBase表
    table.put(puts)

    // 全局连接不关闭: 应该使用连接池。
  }

}
