package com.setapi.etlstreaming

import java.util

import com.alibaba.fastjson.JSON
import com.setapi.JavaUtils.JedisPoolUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
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
  * TODO: 重新分区对存储的意义是：可以将每个分区的数据
  * 一起批量插入到HBase而不需要创建多个HBase连接
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
  * 支持容灾：
  * def createDirectStream[
  * K: ClassTag,      -每条数据KEY的类型
  * V: ClassTag,      -每条数据VALUE的类型
  * KD <: Decoder[K]: ClassTag,   -KEY解码器
  * VD <: Decoder[V]: ClassTag,   -VALUE解码器
  * R: ClassTag] (    -Return的意思，表示处理每条KafkaTopic中数据后得到的值的类型
  * ssc: StreamingContext,
  * kafkaParams: Map[String, String],
  * fromOffsets: Map[TopicAndPartition, Long],            -从哪个偏移量开始读取数据，可用于恢复计算
  * messageHandler: MessageAndMetadata[K, V] => R         -如何从TOPIC中获取数据获取哪些值
  * )
  * -----------------------------------------------------
  * TopicAndPartition：
  * 表示每个分区的唯一标识，一个分区属于一个TOPIC，每个TOPIC中有多个分区
  * MessageAndMetadata：
  * 表示将数据发送到TOPIC后，完整信息封闭类（CaseClass）,包含如下信息
  * KEY，VALUE（MESSAGE），PARTITION（属于哪个分区），OFFSET（分区中的偏移量）
  *
  * ------------------------------------------------------
  * 偏移量存储：
  * 可以存储到:
  * Zookeeper
  * Redis
  * RDBMS
  * HBase
  *
  **/

object EtlKafkaToHBaseTopicRecover {
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

  // REDIS中偏移量,将各分区中最新的偏移量存储在REIDS
  val REDIS_ETL_KEY = "STREAMING_ETL_OFFSETS"

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
      // 设置 Topic 获取最大数据/秒, 它与 interval 的积，是批次数据总量
      .set("spark.streaming.kafka.maxRatePerPartition", "3")
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
    // OFFSET: 从REDIS读取到每个分区的偏移量信息
    val fromOffsets: Map[TopicAndPartition, Long] = {
      // 从REIDS中读取
      // 获取Jedis连接
      val jedis = JedisPoolUtil.getJedisPoolInstance

      // 依据EKY获取所有field/value
      val offSetsMap: util.Map[String, String] = jedis.hgetAll(REDIS_ETL_KEY)

      // 组装MessageAndMetadata->Long实例放入Map(scala)中
      var topicMap = scala.collection.mutable.Map[TopicAndPartition, Long]()
      import scala.collection.JavaConverters._
      for ((field, value) <- offSetsMap.asScala) {
        val splited = field.split("_")
        topicMap += TopicAndPartition(splited(0), splited(1).toInt) -> value.toLong
      }

      // 关闭连接
      JedisPoolUtil.release(jedis)

      // 返回
      topicMap.toMap
    }
    logger.warn("初始的TOPIC与偏移量:" + fromOffsets)

    // 自定义获取的值：默认是K,V.如何从TOPIC中获取数据获取哪些值, 此处获取偏移量及Message(value)
    val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.offset, mam.message())
    // 采用 Direct 方式从 Kafka Topic中读取数据
    val kafkaDStream: InputDStream[(Long,  String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (Long, String)](
        sc,
        kafkaParams,
        fromOffsets,
        messageHandler
      )
    // TODO: 会从异常结束时的位置，继续读取Kafka Topic中的数据
    // TODO: 该功能是否与 checkpoint 的效果一至？未验证，应该是。
    logger.warn("显示20条KAFKA读取到的数据: 每个分区，都有相同的偏移量被看到")
    kafkaDStream.print(20)

    /**
      * DStream.foreachRDD 为处理数据时使用, 返回Unit
      * DStream.transform 为转换数据时使用，返回DStream
      */
    kafkaDStream.foreachRDD((rdd, time) => {
      // TODO: kafkaDStream直接从TOPIC中读取的DStream实例对象
      // TODO: 该RDD为KafkaRDD,其中包含了每个分区数据来源于TOPIC
      // TODO: 中每个分区的偏移量信息 fromOffset, untilOffset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

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

        // 将数据存储到HBase
        partitionId match {
          case 0 => {insertIntoHBase("etlns:etl_alipay", iter)}
          case 1 => {insertIntoHBase("etlns:etl_weixin", iter)}
          case 2 => {insertIntoHBase("etlns:etl_card", iter)}
          case _ => {insertIntoHBase("etlns:etl_other", iter)}
        }

        logger.warn("数据存储到HBase完成: 每个分区连接一次存储")
      })

      // TODO: 当每批次数据插入完成后，更新偏移量信息
      // 获取Jedis连接
      val jedis = JedisPoolUtil.getJedisPoolInstance

      // 更新Redis中的偏移量数据
      for(offsetRange <- offsetRanges) {
        jedis.hset(
          REDIS_ETL_KEY,
          offsetRange.topic + "_" + offsetRange.partition,
          offsetRange.untilOffset.toString
        )
      }
      logger.warn("Kafka Direct Read Offsets be restored into Redis")

      // 关闭连接
      JedisPoolUtil.release(jedis)

    })

//    /**
//      * TODO：上面的另一个写法：以下方法更好？
//      */
//      // 解析DStream数据
//    val orderTypeDStream = kafkaDStream.transform(rdd => {
//      rdd.mapPartitions(_.map {
//        case (_, msg) => {
//          // 解析获取Order数据
//          val orderType = JSON.parseObject(msg).getString("orderType")
//          // 返回
//          (orderType, msg)
//        }
//      })
//    })
//
//    // 将数据重分区，并插入到HBase中
//    orderTypeDStream.foreachRDD((rdd, time) =>
//      rdd
//        .partitionBy(new OrderTypePartitioner)
//        .foreachPartition(iter => {
//          // 由于每个RDD的分区数据被一个Task处理，每个Task 有一个 TaskContext，可获取分区ID
//          val partitionId = TaskContext.getPartitionId()
//          logger.warn(s"===>按分区显示数据partitionId = ${partitionId}")
//          iter.take(3).foreach(println)
//
//          partitionId match {
//            case 0 => {insertIntoHBase("etlns:etl_alipay", iter)}
//            case 1 => {insertIntoHBase("etlns:etl_weixin", iter)}
//            case 2 => {insertIntoHBase("etlns:etl_card", iter)}
//            case _ => {insertIntoHBase("etlns:etl_other", iter)}
//          }
//
//          logger.warn("数据存储到HBase完成: 整个RDD连接一次存储。")
//
//        })
//    )


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
