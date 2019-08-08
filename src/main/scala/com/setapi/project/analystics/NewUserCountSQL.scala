package com.setapi.project.analystics

import java.sql.DriverManager
import java.util.{Calendar, Properties}

import com.setapi.bigdata.java.common.EventLogConstants
import com.setapi.bigdata.java.common.EventLogConstants.EventEnum
import com.setapi.bigdata.java.util.TimeUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
//import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.hadoop.hbase.util.Base64

/**
  *
  * 需求：统计每天的新用户。
  * 1，时间维度：每条进行统计
  * 2，平台维度：名台名称与版本浏览网站所使用的客户端
  * 3，浏览器维度：版本包客户端名称
  *
  * 数据:
  * en, 事件类型,  e_l，第一次加载
  * s_time， 服务器访问时间, 获取时间维度
  * version: 平台版本
  * pl: platfor， 平台名称
  * browerVersion, 浏览器名称
  * browserName, 浏览器名称
  * uuid, 用户ID，无此字段则为脏数据
  *
  * 流程：
  * 基于SparkCore, 从HBase表中读取数据，统计新增用户
  * 按照不同维度进行统计分析
  *
  * 输出:
  * 每天的新用户统计
  *
  * 使用SQL来实现新用户的统计分析
  *
  **/


object NewUserCountSQL {
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
      logger.warn("Usage: NewUserCountSQL process_date")
      System.exit(1)
    }

    /**
      * 创建SparkContext实例对象
      */
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NewUserCountSQL_App")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Put]))
    val sc = SparkContext.getOrCreate(sparkConf)

    /**
      * 从HBase表中读取数据
      * 在此业务中，只需要事件类型为 lunch
      */
    // 数据读取表
    // 处理时间
    val processDate = args(0)
    val time = TimeUtil.parseString2Long(processDate)
    val dateSuffix = TimeUtil.parseLong2String(time, "yyyyMMdd")
    // 表名
    val tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + "_" + dateSuffix

    // 从哪张表中读取数据
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    // TODO: 从HBASE表中查到数据后进行筛选
    // TODO: 可以通过直接读取HFILE文件，避开SCAN，提升性能
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
        CompareFilter.CompareOp.EQUAL,
        Bytes.toBytes(EventEnum.LAUNCH.alias)
      )
    )

    // TODO: 设置Scan扫描器，进行过滤
    conf.set(
      TableInputFormat.SCAN,
      TableMapReduceUtil.convertScanToString(scan)
    )

    // TODP: 不需要这么读，使用下面的 newAPIHadoopRDD 读取为RDD更好
    // val table = conn.getTable(TableName.valueOf(tableName))
    // val result = table.getScanner(scan)

    // TODO: 调用SparContext类中的 NewAPIHadoopRDD
    val eventLogsRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

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
    val dayPlatformBrowserNewUserRDD: RDD[DayPlatfromBrowser] = newUsersRDD.map {
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
        DayPlatfromBrowser(uuid, day, platformDimension, browserDimension)
      }
    }

    // 查看浏览器平台新用户信息
    dayPlatformBrowserNewUserRDD.take(5).foreach {
      case DayPlatfromBrowser(uuid, day, platformDimension, browserDimension) => {
        println(s"uuid=${uuid}, day=${day}, " +
          s"platformDimension=${platformDimension}, " +
          s"browserDimension=${browserDimension}")
      }
    }

    // TODO: 构建SparkSession实例对象
    val spark = SparkSession.builder()
      .config(sc.getConf)
      .config("spark.sql.shuffle.partitions", "6")
      .getOrCreate()
    import spark.implicits._

    // TODO: 将RDD转换为DataFrame
    val dayPlatfromBrowserDF = dayPlatformBrowserNewUserRDD.toDF()

    dayPlatfromBrowserDF.printSchema()
    dayPlatfromBrowserDF.show(5, truncate = false)

    // 缓存数据
    dayPlatfromBrowserDF.persist(StorageLevel.MEMORY_AND_DISK)

    // TODO: 基本维度分析，时间维度 + 平台维度
    logger.warn("基本维度分析，时间维度 + 平台维度: ---->")
    // 使用DSL分析, 调用DataFrame中的API
    dayPlatfromBrowserDF
      .select($"day", $"platformDimension")
      .groupBy($"day", $"platformDimension")
      .count()
      .show(30, truncate = false)

    // 注册为临时视图: 此视图，在当前SparkSession中可用
    dayPlatfromBrowserDF.createOrReplaceTempView("view_tmp_dpb")
    // 注册为全局视图: 与其它SparkSession共享视图, 保存在 global_temp 中
    dayPlatfromBrowserDF.createOrReplaceGlobalTempView("view_tmp_dpb")

    /**
      * TODO：基本维度分析 + 浏览器维度
      */
    logger.warn("基本维度分析 + 浏览器维度: ---->")
    spark.sql(
      """
        |SELECT
        | day, platformDimension, browserDimension, COUNT(1) AS total
        |FROM
        | global_temp.view_tmp_dpb
        |GROUP BY
        | day, platformDimension, browserDimension
      """.stripMargin
    ).show(30, truncate = false)


    /**
      * 离线分析将结果保存到MYSQL
      * 实时数据分析将结果保存到REDIS
      */
    // DataFrame.write.jdbc() 保存数据到MYSQL

    /**
      * MSYQL连接，用于数据写入JDBC
      *
      */
    val (url, props) = {
      val jdbcUrl = "jdbc:mysql://hnamenode:3306"
      val jdbcProps = new Properties()
      jdbcProps.put("user", "root")
      jdbcProps.put("password", "birdhome")

      // 返回
      (jdbcUrl, jdbcProps)
    }

    /**
      * 此应用，如开始运行失败，其中已经插入了数据
      * 再次应用时，结果将追加, SaveMode中的选项不可用
      * -a, 当主键存在时，更新主键
      * -b, 当主键不存在时，插入数据
      * 在MSYQL数据库听SQL语法为：
      * INSER INTO tableName() VALUES () ON
      * DUPLICATE KEY UPDATE field1=value1, field2=value2, ...
      */
    spark.sql(
      """
        |SELECT
        | day, platformDimension, browserDimension, COUNT(1) AS total
        |FROM
        | global_temp.view_tmp_dpb
        |GROUP BY
        | day, platformDimension, browserDimension
      """.stripMargin
    )
      // 降低分区数量
      //.map()
      .coalesce(1)
      .foreachPartition(iter => {
        // 创建MYSQL连接
        Class.forName("com.mysql.jdbc.Driver")
        var conn: java.sql.Connection = null

        try {
          conn = DriverManager.getConnection(url, props.getProperty("user"), props.getProperty("password"))

          // 构建PreparedStatement对象
          val sqlStr =
            """
              |INSERT INTO analysis.day_platform_browser (day, platformDimension, browserDimension, count)
              |VALUES (?, ?, ?, ?)
              |ON DUPLICATE
              |KEY UPDATE day=?, platformDimension=?, browserDimension=?, count=?
            """
              .stripMargin
          val pstmt = conn.prepareStatement(sqlStr)
          // TODO: 针对迭代器中的数据，插入MySQL数据库的表中
          iter.foreach {
            item => {
              pstmt.setString(1, item.get(0).toString)
              pstmt.setString(2, item.get(1).toString)
              pstmt.setString(3, item.get(2).toString)
              pstmt.setString(4, item.get(3).toString)
              pstmt.setString(5, item.get(0).toString)
              pstmt.setString(6, item.get(1).toString)
              pstmt.setString(7, item.get(2).toString)
              pstmt.setString(8, item.get(3).toString)

              // 更新插入
              pstmt.executeUpdate()
            }
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (null != conn) conn.close()
        }
      })

    logger.warn("数据插入完成\n")

    // 释放资源
    dayPlatfromBrowserDF.unpersist()

    // 开发测试，线程休眠，web ui查看
    Thread.sleep(100000000)
    sc.stop()

  }
}



