package com.setapi.examples.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.setapi.JavaUtils.JedisPoolUtil
import com.setapi.examples.Utils.Constant
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{HashPartitioner, SparkConf}
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisConnectionException


/**
  *
  * Created by ShellMount on 2019/8/1
  *
  * 需求：实时统计分析订单数据
  * 窗口统计TOP5订单量与累加和
  *
  *
  * 贷出函数：LoanFunction
  * 资源的管理，如创建关闭 StreamingContext
  * 用户函数：UserFunction
  * 用户业务实现的地方
  *
  *
  * SparkStreaming优化点：
  * 1, 采用 Direct 方式拉取 Kafka Top 数据时，设置最大消费数据量
  *       spark.streaming.kafka.maxRatePerPartition
  * 每秒钟获取每个分区最大的条目数
  * 2, 数据本地性等待时间
  *       spark.locality.wait
  * RDD中每个分区数据处理的时候，考虑处理数据所在
  * 的位置和需要的资源的本地性，尽量达到最好的性能
  * 为了实现最好性能往往会等待资源。默认值：3秒，
  * 对Streaming实时应用来说过长，本地性无需考虑。
  * 3, SparkCore中性能优化，可以全部考虑使用。如，
  * a,spark.serializer
  * 设置Kryo序列化
  * b, 使用 xxPartition 参数
  * c, 使用aggreateByKey 函数聚合，少使用 groupByKey
  *
  * 4, 配置反压机制：程序自动依据每批次处理数据的时间，自动
  * 调整下一批次处理的数据量，以便在 BatchInterval 的时间
  * 范围内处理完所有数据，进行实时的分析
  *       spark.streaming.backpressure.enable
  *       spark.streaming.backpressure.initialRate
  * 可不配置, 因已设置：
  *       spark.streaming.kafka.maxRatePerPartition
  *
  * 5, 设置 Driver / Executor 中JVM GC 策略及内存分配
  *       spark.driver.extraJavaOptions, 对Driver设置
  *       spark.executor.extraJavaOptions, 对Executor
  *
  **/

object RealOrderStreamingConsumer {
  val logger = Logger.getLogger(this.getClass)
  // 检查点
  val CHECK_POINT_PATH = "/tmp/spark/streaming/my_checkpoint/ckpt-00011"
  // BATCH INTER
  val STREAMING_BATCH_INTERVAL = Seconds(5)
  // 窗口时间间隔
  val STREAMING_WINDOW_INTERVAL = STREAMING_BATCH_INTERVAL * 3
  // 滑动时间间隔
  val STREAMING_SLIDER_INTERVAL = STREAMING_BATCH_INTERVAL * 3

  // 存储 Redis 中实时统计销售额
  val REDIS_KEY_ORDERS_TOTAL_PRICE = "orders:total:price"


  def main(args: Array[String]): Unit = {
    // 调用贷出函数，传递用户函数
    sparkOperation(args)(processStreamingData)
  }

  /**
    * 供出波函数：管理SC流式上下文对象的创建与关闭
    *
    * @param args
    * 应用程序接收的参数
    * @param operation
    * 用户函数：业务逻辑的处理
    */
  def sparkOperation(args: Array[String])(operation: StreamingContext => Unit): Unit = {
    /**
      * 初次运行 SparkStreaming 时创建新的实例
      */
    val creatingFunc = () => {
      // TODO: 构建 SparkConf 配置
      val sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("RealOrderStreamingConsumer")
        // 设置 Topic 获取最大数据/秒, 它与 interval 的积，是批次数据总量
        .set("spark.streaming.kafka.maxRatePerPartition", "5000")
        .set("spark.locality.wait", "100ms")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[SaleOrder]))
        // 反压机制
        .set("spark.streaming.backpressure.enable", "true")
        .set("spark.driver.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+UseG1GC")
      // TODO: 设置StreamingContext 参数
      val ssc = new StreamingContext(sparkConf, STREAMING_BATCH_INTERVAL)

      // TODO: 处理数据
      operation(ssc)
      // TODO: 设置检查点
      ssc.checkpoint(CHECK_POINT_PATH)
      ssc
    }

    //创建 StreamingContext 实例
    var context: StreamingContext = null
    try {
      // 创建实例
      context = StreamingContext.getActiveOrCreate(CHECK_POINT_PATH, creatingFunc)

      // 设置日志级别
      context.sparkContext.setLogLevel("WARN")

      // 启动
      context.start()
      context.awaitTermination()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != context) context.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  /**
    * 用户函数：业务逻辑的处理
    * 实时获取KAFKA TOPIC中的数据
    *
    * @param ssc
    * 流实例对象
    */
  def processStreamingData(ssc: StreamingContext): Unit = {
    // TODO: 从KAFKA读取数据, Direct方式
    val kafkaParams = Map(
      "metadata.broker.list" -> Constant.METADATA_BROKER_LIST,
      "auto.offset.reset" -> Constant.AUTO_OFFSET_RESET)

    // TOPIC SETTING
    val topics = Set(Constant.TOPIC)

    val kfkDStream: DStream[(String, String)]
    = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics
    )

    // TODO: 解析数据
    val orderDStream = kfkDStream.transform(
      rdd => {
        rdd.map {
          case (_, message) => {
            val mapper = ObjectMapperSingle.getInstance()
            // 解析JSON
            val saleOrder = mapper.readValue(message, classOf[SaleOrder])
            // 返回
            (saleOrder.provinceId, saleOrder)
          }
        }
      }
    )

    /**
      * 更多地使用 xxPartition(s)
      */
    val orderDStream2 = kfkDStream.transform(
      rdd => {
        rdd.mapPartitions(iter => {
          iter.map { case (_, message) => {
            val mapper = ObjectMapperSingle.getInstance()
            // 解析JSON
            val saleOrder = mapper.readValue(message, classOf[SaleOrder])
            // 返回
            (saleOrder.provinceId, saleOrder)
          }
          }
        })
      })
    // orderDStream.print()

    // TODO: 分析处理: 实时累加统计各省份销售额度
    /**
      * updateStateByKey[S: ClassTag](updateFunc: (Time, K, Seq[V], Option[S]) => Option[S],partitioner: Partitioner,
      * rememberPartitioner: Boolean,
      * initialRDD: Option[RDD[(K, S)]] = None)
      * Time: 批处理BatchInteral
      * K: 当前要处理的 Key
      * Seq[V]: 当前 Key 所有 Value
      * Option[S]: 当前 Key 之前的状态值
      */
    val orderPriceDStream: DStream[(Int, Float)] = orderDStream.updateStateByKey(
      (batchTime: Time, provinceId: Int, orders: Seq[SaleOrder], state: Option[Float]) => {
        /**
          * 可以依据此处的 Time 作 与时间相关的进一步处理
          * 时间也可以来自 order 中的字段。
          * 当前批次KEY的状态信息、之前的值、返回更新后的值
          */
        val currentOrderPrice = orders.map(_.orderPrice).sum
        val previousPrice = state.getOrElse(0.0f)
        Some(previousPrice + currentOrderPrice)
      },
      new HashPartitioner(ssc.sparkContext.defaultParallelism),
      rememberPartitioner = true
    )

    // TODO: 输出数据
    orderPriceDStream.print()

    // TODO: 存储结果，REDIS
    orderPriceDStream.foreachRDD(
      (rdd, time) => {
        // 处理时间
        val batchTime = new SimpleDateFormat("yyy-MM-dd HH:mm:SS").format(new Date(time.milliseconds))
        logger.warn("-----------------------------")
        logger.warn(s"batchTime: ${batchTime}")
        logger.warn(s"rdd.partitions: ${rdd.partitions.length}")
        logger.warn("-----------------------------")

        if (!rdd.isEmpty()) {

          // 降低分区数，将结果存储到 Redis，数据类型：HASH
          rdd.coalesce(1).foreachPartition(iter => {

            var jedis: Jedis = null

            // TODO: 获取 Jedis 连接
            try {
              jedis = JedisPoolUtil.getJedisPoolInstance

              // TODO: 将统计销售订单额存储到 Redis 中, 此处 foreach 不能使用 ()??

              iter.foreach {
                case (provinceId, orderTotal) =>
                  jedis.hset(REDIS_KEY_ORDERS_TOTAL_PRICE, provinceId.toString, orderTotal.toString)
              }
              logger.warn("Redis Written.")
            } catch {
              case e: JedisConnectionException => logger.warn("Jedis connecting false.")
              case o: Exception => o.printStackTrace()
            } finally {
              // TODO：关闭连接
              JedisPoolUtil.release(jedis)
            }
          })
        }
      }
    )

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
      * 排序等更复杂的运算
      * 实时统计销售额最高的 10 个省份
      */
    orderDStream
      // 设置窗口大小与滑动大小
      .window(STREAMING_WINDOW_INTERVAL, STREAMING_SLIDER_INTERVAL)
      //集成SparkSQL计算
      .foreachRDD((rdd, time) => {
      // 处理时间
      val batchTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time.milliseconds))
      logger.warn(s"batchTime for saving to mysql: ${batchTime}")
      if (!rdd.isEmpty()) {
        // RDD 数据缓存
        rdd.cache()

        // TODO: 构建 SparkSession 对象
        val spark = SparkSession.builder()
          .config(rdd.sparkContext.getConf)
          .config("spark.sql.shuffle.partitions", "6")
          .getOrCreate()
        import spark.implicits._

        // TODO: 将 RDD 转换为 Dataset or DataFrame
        // TODO: 由于RDD中的数据类型SaleOrder为CaseClass隐匿转换
        val saleOrderDF = rdd.map(_._2).toDF()

        // TODO: 编写DSL
        val top10ProvinceOrderCountDF = saleOrderDF.groupBy($"provinceId").count().orderBy($"count".desc).limit(10)
        logger.warn(s"top10ProvinceOrderCountDF: ")
        top10ProvinceOrderCountDF.show()

        // TODO: 存储到MySql 中
        top10ProvinceOrderCountDF.write
          .mode(SaveMode.Overwrite)
          .jdbc(url, "analysis.order_top10_count", props)

        // RDD 释放
        rdd.unpersist()
      }
    })


    /**
      * 在RDD中求出TOP10销售业绩的省
      *
      */
    orderDStream.foreachRDD((rdd, time) => {
      logger.warn("使用 aggregateByKey 求出TOP5 : ")
      val topN: RDD[(Int, Int)] = rdd.aggregateByKey(0)(_ + _.provinceId, _ + _)
        .sortBy(_._2, false)
      topN.take(5).foreach(println)
    })
  }
}
