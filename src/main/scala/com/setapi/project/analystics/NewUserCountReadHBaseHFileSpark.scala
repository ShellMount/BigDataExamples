package com.setapi.project.analystics

import java.sql.DriverManager
import java.util.Calendar

import com.setapi.bigdata.java.common.EventLogConstants
import com.setapi.bigdata.java.common.EventLogConstants.EventEnum
import com.setapi.bigdata.java.util.TimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil, TableMapper, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, CompareOperator, HBaseConfiguration}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  *
  * Created by ShellMount on 2019/8/6
  *
  * 从HBase中读取数据
  * 通过SparkCore读取HFile数据文件
  * 读取HBase的快照文件
  * 即使HBase服务关闭了，仍然可以执行该方案
  *
  **/

object NewUserCountReadHBaseHFileSpark {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  // HBASE连接
  val conf = HBaseConfiguration.create()
  val conn = ConnectionFactory.createConnection(conf)
  // 定义连接HBase CLuster
  val admin = conn.getAdmin.asInstanceOf[HBaseAdmin] //父类转子类

  def main(args: Array[String]): Unit = {

    // TODO: 需要传递一个
    if (args.length < 1) {
      logger.warn("Usage: NewUserCountReadHBaseHFileSpark process_date")
      System.exit(1)
    }

    /**
      * 创建SparkContext实例对象
      */
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NewUserCountReadHBaseHFileSpark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Put]))
    val sc = SparkContext.getOrCreate(sparkConf)

    // 处理时间格式
    val time: Long = TimeUtil.parseString2Long(args(0)) // "yyyy-MM-dd"
    val dateSuffix: String = TimeUtil.parseLong2String(time, "yyyyMMdd")
    // table name
    val tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + "_" + dateSuffix
    // b. 设置从哪张表读取数据 -> TODO: 实际对应的是表的快照信息
    val snapshotName = "snapshot_" + tableName


    // 构建扫描器
    // TODO: 从HBASE表中查到数据后进行筛选
    // TODO: 可以通过直接读取HFILE文件，避开HBase服务，提升性能
    // 创建Scan实例对象，扫描表中的数据
    // 创建Scan实例对象，扫描表中的数据
    val scan = new Scan()

    // 设置列簇
    val FAMILY_NAME = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME
    scan.addFamily(FAMILY_NAME)

    // 查询的列
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME))
    scan.addColumn(FAMILY_NAME, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION))

    // 设置过滤器: 事件类型必须为 e_l
    scan.setFilter(
      new SingleColumnValueFilter(
        FAMILY_NAME,
        Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
        CompareOperator.EQUAL,
        Bytes.toBytes(EventEnum.LAUNCH.alias)
      )
    )

    // TODO: 设置Scan扫描器，进行过滤
    conf.set(
      TableInputFormat.SCAN,
      TableMapReduceUtil.convertScanToString(scan)
    )

    // TODO: TableSnapshot设置，从哪个快照读取数据
    val restoreDir = new Path("/bigdata/snapshots/event_logs/")
    val job = NewAPIHadoopJob.getInstance(conf)
    //    TableSnapshotInputFormat.setInput(
    //      job,
    //      snapshotName,
    //      restoreDir
    //    )

    // TODO: 或者这么配置
    import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
    TableMapReduceUtil.initTableSnapshotMapperJob(
      snapshotName,
      scan,
      classOf[TableMapper[_, _]],
      classOf[ImmutableBytesWritable],
      classOf[Result],
      job,
      true,
      restoreDir,
      null,
      1
    )


    // TODO: 读取HFile文件，需要事先创建SNAP
    /**
      * hbase(main):008:0> snapshot 'event_logs_20151220', 'snapshot_event_logs_20151220'
      * hbase(main):009:0> list_snapshots
      * => ["snapshot_event_logs_20151220"]
      **/

    /**
      * 调用SparContext类中的 NewAPIHadoopRDD
      */
    val eventLogsRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TableSnapshotInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )


    // TODO: 这种方式读出的数据，变少了!!!!!!!!???????
    // TODO: SNAPSHOT中数据并未减少，只是这里读取到的变少了
    logger.warn(s"数据条数: ${eventLogsRDD.count()}")

    eventLogsRDD.take(5).foreach {
      case (key, result) =>
        println(s"RowKey = ${Bytes.toString(key.get())}")
        for (cell <- result.rawCells()) {
          val cf = Bytes.toString(CellUtil.cloneFamily(cell))
          val column = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          println(s"\t${cf}:${column} = ${value}  --> ${cell.getTimestamp}")
        }
    }


    // TODO: 对读取到的数据作转换
    val newUsersRDD = eventLogsRDD.mapPartitions(iter => {
      iter.map {
        case (key, result: Result) => {
          // 获取 RowKey
          val rowKey = Bytes.toString(key.get())

          // 获取所有字段的值
          val uuid = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)))
          val serverTime = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)))
          val platformName = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)))
          val platformVersion = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)))
          val browserName = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)))
          val browserVersion = Bytes.toString(result.getValue(FAMILY_NAME,
            Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)))

          // 返回元组
          (uuid, serverTime, platformName, platformVersion, browserName, browserVersion)
        }
      }
    })
      // 过滤无效数据: uuid 或 serverTime 为空
      .filter(tuple => {
      null != tuple._1 & null != tuple._2
    })
    // 对客户端平台及浏览器处理
    val dayPlatformBrowserNewUserRDD = newUsersRDD.map {
      case (uuid, serverTime, platformName, platformVersion, browserName, browserVersion) => {

        // 获取当前处理日期为当月的第几天
        val calendar = Calendar.getInstance()
        calendar.setTimeInMillis(TimeUtil.parseNginxServerTime2Long(serverTime))
        val day = calendar.get(Calendar.DAY_OF_MONTH)

        // 平台维度作处理
        var platformDimension: String = ""
        if (StringUtils.isBlank(platformName)) {
          platformDimension = "unknown:unknown"
        } else if (StringUtils.isBlank(platformVersion)) {
          platformDimension = platformName + ":unknow"
        } else {
          platformDimension = platformName + ":" + platformVersion
        }

        // 浏览器维度的组合
        var browserDimension: String = ""
        if (StringUtils.isBlank(browserName)) {
          browserDimension = "unknown:unknown"
        } else if (StringUtils.isBlank(browserVersion)) {
          browserDimension = browserName + ":unknow"
        } else {
          browserDimension = browserName + ":" + browserVersion
        }

        // 返回元组
        (uuid, day, platformDimension, browserDimension)
      }
    }

    // 查看浏览器平台新用户信息
    dayPlatformBrowserNewUserRDD.take(5).foreach {
      case (uuid, day, platformDimension, browserDimension) => {
        println(s"uuid=${uuid}, day=${day}, " +
          s"platformDimension=${platformDimension}, " +
          s"browserDimension=${browserDimension}")
      }
    }

    // 缓存数据: 多次使用的数据进行缓存
    dayPlatformBrowserNewUserRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // TODO: 基本维度分析，时间维度 + 平台维度
    val dayPlatformNewUserCount = dayPlatformBrowserNewUserRDD
      // 提取字段：组合维度，标记1次
      .mapPartitions(_.map {
      case (uuid, day, platformDimension, browserDimension) => {
        ((day, platformDimension), 1)
      }
    })
      .reduceByKey(_ + _)

    logger.warn(s"每天来自不同平台的新用户: ${dayPlatformNewUserCount.count()}")
    dayPlatformNewUserCount.foreachPartition(_.foreach(println))

    /**
      * TODO：基本维度分析 + 浏览器维度
      */
    val dayBrowserNewUserCount = dayPlatformBrowserNewUserRDD
      // 提取字段：组合维度，标记1次
      .mapPartitions(_.map {
      case (uuid, day, platformDimension, browserDimension) => {
        ((day, browserDimension), 1)
      }
    })
      .reduceByKey(_ + _)

    logger.warn(s"每天来自不同浏览器的新用户: ${dayBrowserNewUserCount.count()}")
    dayBrowserNewUserCount.foreachPartition(_.foreach(println))

    /**
      * 离线分析将结果保存到MYSQL
      * 实时数据分析将结果保存到REDIS
      */
    val dayPlatformBrowserNewUserRDDCount = dayPlatformBrowserNewUserRDD
      .mapPartitions(_.map {
        case (uuid, day, platformDimension, browserDimension) => {
          ((day, platformDimension, browserDimension), 1)
        }
      })
      .reduceByKey(_ + _)

    dayPlatformBrowserNewUserRDDCount.coalesce(1)
      .foreachPartition(iter => {
        Try {
          // 获取连接
          val connDb = {
            // 加载驱动
            Class.forName("com.mysql.jdbc.Driver")
            val url = "jdbc:mysql://hnamenode:3306/analysis"
            val user = "root"
            val password = "birdhome"
            // 返回链接
            DriverManager.getConnection(url, user, password)
          }

          // 获取数据库原始事务
          val oldAutoCommit = connDb.getAutoCommit

          // TODO: 设置事务，针对当前分区数据，工么都插入，要么都不
          connDb.setAutoCommit(false)

          // 创建 PrepareStatement 实例对象
          // 插入数据库SQL，实现 Insert or Update
          val sqlStr =
          """
            |INSERT INTO analysis.day_platform_browser (day, platformDimension, browserDimension, count)
            |VALUES (?, ?, ?, ?)
            |ON DUPLICATE
            |KEY UPDATE count=VALUES(count)
          """.stripMargin

          val pstmt = connDb.prepareStatement(sqlStr)

          // 对数据进行迭代输出
          iter.foreach { case ((day, platformDimension, browserDimension), count) => {
            // 设置列的值
            pstmt.setInt(1, day)
            pstmt.setString(2, platformDimension)
            pstmt.setString(3, browserDimension)
            pstmt.setInt(4, count)
            pstmt.addBatch()

          }
          }

          // 提交批次
          pstmt.executeBatch()

          // 手动提交事务，要么全部插入，要么全部放弃
          connDb.commit()

          // 如果成功返回Connection
          (oldAutoCommit, connDb)
        } match {
          case Success((oldAutoCommit, connDb)) => {
            // 当数据插入成功后恢复原先数据库的事务相关设置
            Try(connDb.setAutoCommit(oldAutoCommit))
            if (null != connDb) connDb.close()
            logger.warn("数据事务插入MYSQL完成\n")
          }
          case Failure(exception) => {
            logger.warn("数据事务插入出错! \n" + exception.printStackTrace())
          }
        }
      })


    // TODO: 数据读取条目数不准确的事，未解决。
    logger.warn("数据读取条目数不准确的事，未解决! \n")

    // 释放资源
    dayPlatformBrowserNewUserRDD.unpersist()

    // 开发测试，线程休眠，web ui查看
    Thread.sleep(100000000)
    sc.stop()

  }
}
