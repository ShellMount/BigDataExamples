package com.setapi.project.etl

import java.util
import java.util.zip.CRC32

import com.setapi.bigdata.java.common.EventLogConstants
import com.setapi.bigdata.java.common.EventLogConstants._
import com.setapi.bigdata.java.util.{LogParser, TimeUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Durability, HBaseAdmin, Put, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ShellMount on 2019/8/2
  *
  * 基于SparkCore框架读取HDFS中的日志文件，进行ETL操作
  * 最终将数据写入HBASE中
  * 1，为什么选择ETL表中
  * 采集的数据，包含EVENT事件类型的数据，不同的EVENT数据
  * 的字段不一样。且数据量较大
  *
  * 2，HBASE表的设计？
  * 每天的日志数据ETL到一张表中，每天分析一次即可
  *
  * 每次ETL数据的时候，创建一张表：event_logs_ + 日期
  *
  * 每次判断表是否存在，如存在则先删除
  * --创建表时考虑创建预分区，使用数据存储到多个region中
  * 减少写热点
  * --考虑表中的数据压缩，使用snappy或Lz4压缩
  *
  * RowKey设计原则：
  * 唯一性
  * 结合业务：时间、EventType
  * RowKey = 服务器时间戳 + CRC32编码（用户ID、会员ID、
  * 事件名称）
  *
  * 列簇: 一个列簇,info
  * 解析每条日志获取的Map中的Key为列名，Value为其值
  * 考虑将服务器时间戳设置为每列数据的版本号
  *
  *
  * 优化：广播变量
  *
  **/


object EtlToHbaseSpark {
  Logger.getRootLogger.setLevel(Level.WARN)

  val logger = Logger.getLogger(this.getClass)

  /**
    * 创建HBASE表，判断是否存在，存在的话，先删除后创建
    *
    * @param processDate
    * 处理的日期，2019-03-03
    * @param conf
    * HBase Client需要的配置信息
    * @return
    * 表名称
    */
  def createHBaseTable(processDate: String, conf: Configuration): String = {
    // 处理时间
    val time = TimeUtil.parseString2Long(processDate)
    val dateSuffix = TimeUtil.parseLong2String(time, "yyyyMMdd")

    // 表名
    val tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + "_" + dateSuffix

    // 定义连接HBase CLuster
    var conn: Connection = null
    var admin: HBaseAdmin = null

    try {
      conn = ConnectionFactory.createConnection(conf)
      admin = conn.getAdmin.asInstanceOf[HBaseAdmin] //父类转子类

      // 存在判断
      if (admin.tableExists(TableName.valueOf(tableName))) {
        admin.disableTable(TableName.valueOf(tableName))
        admin.deleteTable(TableName.valueOf(tableName))
      }

      // 创建表的描述符
      val desc = new HTableDescriptor(TableName.valueOf(tableName))
      val familyDesc = new HColumnDescriptor(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME)

      /**
        * 针对列簇进行属性设置
        *
        */
      // 由于主要是分析数据，而不是查询，所以禁用查询缓存
      familyDesc.setBlockCacheEnabled(false)
      // 设置压缩
      familyDesc.setCompressionType(Compression.Algorithm.SNAPPY)
      // 向表中添加列簇
      desc.addFamily(familyDesc)
      // 设置预分区，对对整个表，不针对某个列簇
      // key 为 RowKey 前缀
      val splitKeys = Array(
        Bytes.toBytes("1450570713450_"),
        Bytes.toBytes("1450570137367_"),
        Bytes.toBytes("1450571387346_"),
        Bytes.toBytes("1450571718462_")
      )
      admin.createTable(desc,
        splitKeys
      )
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if(null != admin) admin.close()
      if(null != conn) conn.close()
    }

    tableName
  }


  /**
    * 手动刷新MemStore中的数据到StoreFile
    * @param tableName
    *                  表名称
    */
  def flushHBaseTable(tableName: String, conf: Configuration) = {
    // 定义连接HBase CLuster
    var conn: Connection = null
    var admin: HBaseAdmin = null

    try {
      conn = ConnectionFactory.createConnection(conf)
      admin = conn.getAdmin.asInstanceOf[HBaseAdmin] //父类转子类

      // 刷新数据
      admin.flush(TableName.valueOf(tableName))
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if(null != admin) admin.close()
      if(null != conn) conn.close()
    }

  }

  /**
    * @param args 传入处理的日期参数
    *
    */
  def main(args: Array[String]): Unit = {

    // TODO: 需要传递一个
    if (args.length < 1) {
      logger.warn("Usage: EtlToHbaseSpark process_date")
      System.exit(1)
    }

    /**
      * 创建SparkContext实例对象
      */
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("EtlToHbaseSpark_App")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Put]))
    val sc = SparkContext.getOrCreate(sparkConf)

    /**
      * TODO: 依据传入的处理日期，读取存储在HDFS上的日志文件
      */
    val eventLogsRDD = sc.textFile(s"/bigdata/datas/usereventlogs/${args(0)}/",
      minPartitions = 3
    )

    logger.warn(s"Count = ${eventLogsRDD.count()}\n")
    logger.warn(s"this first line:\n${eventLogsRDD.first()}\n")

    /**
      * 解析日志，格式：
      * IP^A服务器时间^Ahost^A请求参数^
      *
      * 解析日志存储到RDD[(key, MAP)]中
      * MAP数据对存储到HBASE更亲和
      **/
    val parseEventLogsRDD = eventLogsRDD.mapPartitions(iter => {
      iter.map(log => {
        // 调用工具解析得到 Map 集合
        val logInfo: util.Map[String, String] = new LogParser().handleLogParser(log)

        // 获取事件的类型
        val eventAlias = logInfo.get(LOG_COLUMN_NAME_EVENT_NAME)

        (eventAlias, logInfo)
      })
    })

    logger.warn(s"Parse Log : ${parseEventLogsRDD.first()}\n")

    /**
      * eventTypeList 的量很大时，将影响性能
      * 其将分发到每个Executor上的每个TASK
      * 处理方案：广播变量，累加器
      */
    val eventTypeList = List(
      EventEnum.LAUNCH,
      EventEnum.PAGEVIEW,
      EventEnum.EVENT,
      EventEnum.CHARGEREQUEST,
      EventEnum.CHARGEREFUND,
      EventEnum.CHARGESUCCESS
    )
    // TODO: 通过广播将事件类型的列表广播给Executors
    val eventTypeListBroadcast = sc.broadcast(eventTypeList)

    // TODO: 过滤无效数据（通用性，其它过虑在数据分析时进行）和数据转换
    // 过滤事件类型 EventType 不存在、和解析 Map 为空的数据
    val eventPutsRDD = parseEventLogsRDD
      .filter {
        case (eventAlias, logInfo) =>
          //logInfo.size() != 0 && eventTypeList.contains(EventEnum.valueOfAlias(eventAlias))
          logInfo.size() != 0 && eventTypeListBroadcast.value.contains(EventEnum.valueOfAlias(eventAlias))
      }
      .mapPartitions(iter => {
        iter.map { case (eventAlias, logInfo) =>
          // TODO: HBASE表设计
          // 表的主键 RowKey
          val rowKey: String = createRowKey(
            TimeUtil.parseNginxServerTime2Long(logInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)),
            logInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID),
            logInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID),
            eventAlias
          )

          // 创建Put对象，添加列名与列值
          val put = new Put(Bytes.toBytes(rowKey))

          // TODO: 需要将JAVA中MAP的集合转换为SCALA的MAP
          import scala.collection.JavaConversions._
          for ((k, v) <- logInfo) {
            put.addColumn(
              EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME,
              Bytes.toBytes(k.toString),
              Bytes.toBytes(v.toString)
            )
          }

          // TODO: 设置数据不写入WAL, 提升性能
          put.setDurability(Durability.SKIP_WAL)

          // 返回二元组
          (new ImmutableBytesWritable(put.getRow), put)
        }
      })


    logger.warn("准备统计COUNT, 本行输出 null, 原因未明")
    logger.warn(s"Transformation Count = ${eventPutsRDD.count()}\n")
    logger.warn("统计COUNT结束")
    logger.warn(s"-->${eventPutsRDD.first()}\n")

    // TODO: 保存到HBASE
    /**
      * 由于ETL每天执行一次，失败时再执行
      * 对原始数据处理后，存储到HBASE中
      * 表名称：
      * create 'event_logs_20190803', 'info'
      */
    val conf = HBaseConfiguration.create()
    val tableName = createHBaseTable(args(0), conf)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    logger.warn("存储开始: HadoopFiles\n")

    // conf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName)

    // TODO: 存储至HDFS
    eventPutsRDD.saveAsNewAPIHadoopFile(
      "/bigdata/etl/hbase/usereventlogs/etl_" + System.currentTimeMillis(),
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],  // 依赖于库: hbase-mapreduce
      conf
    )
    logger.warn("存储完成: HadoopFiles\n")

    // TODO: 存储完成后，手动调用 HBase Flush
    flushHBaseTable(tableName, conf)

    // 开发测试，线程休眠，web ui查看
    Thread.sleep(100000000)
    sc.stop()

  }

  /**
    * 依据字段信息构建 RowKey
    *
    * @param time
    * 访问服务器的时间
    * @param uuid
    * 用户ID，访问站点时，生成的全局ID
    * @param umd
    * 会员ID
    * @param eventAlias
    * 事件别名
    * @return
    * RowKey
    */
  def createRowKey(time: Long, uuid: String, umd: String, eventAlias: String): String = {
    // 创建StringBuilder实例对象，用于拼接字符串
    val sBuilder = new StringBuilder()
    sBuilder.append(time + "_")

    // 创建CRC32实例对象，进行字符串编码，将字符串转换为Long
    val crc32 = new CRC32()
    // 重置
    crc32.reset()
    if (StringUtils.isNotBlank(uuid)) {
      crc32.update(Bytes.toBytes(uuid))
    }
    if (StringUtils.isNotBlank(umd)) {
      crc32.update(Bytes.toBytes(umd))
    }
    if (StringUtils.isNotBlank(uuid)) {
      crc32.update(Bytes.toBytes(uuid))
    }

    crc32.update(Bytes.toBytes(eventAlias))

    sBuilder.append(crc32.getValue)
    sBuilder.toString()
  }
}

