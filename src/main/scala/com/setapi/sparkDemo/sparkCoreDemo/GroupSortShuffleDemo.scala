package com.setapi.sparkDemo.sparkCoreDemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  *
  * Created by ShellMount on 2019/8/13
  *
  * Spark Application 编程模板
  *
  * run on cluster:
  * ./bin/spark-submit --master spark://192.168.0.211:7077 \
  * --class com.setapi.sparkDemo.sparkCoreDemo.ModuleSpark ../api.jar
  *
  * >>>
  * you can run app on deploy-mode of cluster :
  * ./spark-2.4.3-bin-hadoop2.7/bin/spark-submit \
  * --master spark://192.168.0.211:7077 \
  * --deploy-mode cluster \
  * --class com.setapi.sparkDemo.sparkCoreDemo.ModuleSpark  \
  * --jars \
  * hdfs://hnamenode:9000/sparkapps/AnalisysFromHiveFilesSpark-depends.jars/uasparser-0.6.1.jar,\
  * hdfs://hnamenode:9000/sparkapps/AnalisysFromHiveFilesSpark-depends.jars/hbase-common-2.2.0.jar,\
  * hdfs://hnamenode:9000/sparkapps/AnalisysFromHiveFilesSpark-depends.jars/jregex-1.2_01.jar \
  * hdfs://hnamenode:9000/sparkapps/api.jar
  *
  * >>>
  * can set resources with submit:
  * >> client deploy-mode:
  * ./bin/spark-submit --master spark://192.168.0.211:7077 \
  * --class com.setapi.sparkDemo.sparkCoreDemo.ModuleSpark \
  * --deploy-mode client \
  * --driver-memory 512M \
  * --executor-memory 1g \
  * --executor-cores 1 \
  * --total-executor-cores 2 \
  * ../api.jar
  *
  * >> cluster deploy-mode:
  * ./bin/spark-submit --master spark://192.168.0.211:7077 \
  * --class com.setapi.sparkDemo.sparkCoreDemo.ModuleSpark \
  * --deploy-mode cluster \
  * --driver-memory 512M \
  * --driver-cores 2 \
  * --executor-memory 1g \
  * --executor-cores 1 \
  * --total-executor-cores 2 \
  * ../api.jar
  *
  * >>> 特别的设置优化点
  * 运行在standalone模式下时，work目录的设置： ${SPARK_HOME}/conf/spark-env.sh
  * SPARK_WORKER_DIR     ： 默认在 ${SPARK_HOME}/work
  * SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true \
  * -Dspark.worker.cleanup.interval=1800 \
  * -Dspark.worker.cleanup.appDataTt=7*24*3600"
  *
  * >>> 运行日志保留: 关闭APP后，在WEBUI：18080中再次打开APP运行JOB日志
  * 默认配置文件中设置： ${SPARK_HOME}/spark-defaults.conf
  * SPARK_HISTORY_OPTS
  * spark.eventLog.enabled  true
  * spark.eventLog.dir      hdfs://hnamenode:9000/datas/spark-running-logs/
  * spark.eventLog.compress true
  *
  * OR：
  * ${SPARK_HOME}/conf/spark-env.sh
  * export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 \
  * -Dspark.history.retainedApplications=3 \
  * -Dspark.history.fs.logDirectory=hdfs:/hnamenode:9000/datas/spark-running-logs
  * -Dspark.history.fs.cleaner.enabled=true \
  * "
  *
  * 也可在SparkConf中set
  *
  *
  * 需求：
  * 先按照第一个字段分组，且排序
  * 在组内按照第二个字段排序
  * 并获取组内TOP3
  *
  **/

object GroupSortShuffleDemo {
  // 日志设置
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)

  /**
    * 如果Spark APP运行在本地：DriverProgram
    * JVM Process
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    /**
      * SparkContext 对象, Spark 分析的入口
      */
    val conf = new SparkConf()
      .setAppName("GroupSortShuffleDemo")
      // 本地开发环境设置为local mode, 2线程
      // 实际部署的时候，通过提交命令行进行设置
      // 开发环境设置：-Dspark.master=spark://192.168.0.211:7077
      .setMaster("local[2]")
    // 设置记录此应用的事件EventLog
    // 该设置，会让APP优先寻找与日志相关的内容
    // 且会影响到  SparkContext.getOrCreate创建时的动作，也会先去寻找相关的文件
    // .set("spark.eventLog.enabled", "true")
    // .set("spark.eventLog.compress", "true")
    // .set("spark.eventLog.dir", "hdfs://hnamenode:9000/datas/spark-running-logs/ModuleSpark")

    // 创建SparkContext上下文对象
    val sc = SparkContext.getOrCreate(conf)

    /**
      * 读取数据
      */
    val linesRDD: RDD[String] = sc.parallelize(text.split("\n"))
    val filterRDD = linesRDD.filter(_.trim.length > 0)
    val inputRDD = filterRDD
      .mapPartitions(iter => {
        iter.map { line => {
          val splitted = line.split(" ")
          (splitted(0), splitted(1).trim.toInt)
        }
        }
      })

    // 缓存
    inputRDD.persist(StorageLevel.MEMORY_AND_DISK)
    println(inputRDD.count())
    println("the first tuple is: " + inputRDD.first())


    /**
      * 处理数据
      */
    val sortTopRDD: RDD[(String, List[Int])] = inputRDD
      // 分组
      .groupByKey()
      // 对第二个字段排序, 分组后不能直接对第二个字段排序了
      .map {
      case (key, iter) => {
        /**
          * 通常iter自身的操作函数不足以满足需求时
          * 将iter转为 toList 获取更多的操作函数
          * 这里的分组后，会将同一个KEY的数据，放在一个新的分区中，后续处理时对应着一个TASK
          * 如果某个组的数据特别多时，就会出现数据倾斜，极可能出现OOM或很慢
          * 因此有必要分阶段聚合操作（分阶段排序）
          */
        val sortList = iter.toList.sorted.takeRight(3).reverse
        (key, sortList)
      }
    }

    // 对第一个字段排序: 此时的排序不是全局的
    // 排序操作，需要全局化!!!!
    println("-------------")
    sortTopRDD.sortByKey().foreach(println)

    println("-------------")
    sortTopRDD.coalesce(1).sortByKey().foreach(println)


    println("============ 分阶段进行排序 ============")
    val sortTopRDD2 = inputRDD
      /**
        * 第一阶段分组排序
        */
      // 打乱原始KEY
      .map(tuple => {
      // 随机数据实例
      val random = new Random(2) // 要么是0， 要么是1
      (random + "_" + tuple._1, tuple._2)
    })
      .groupByKey()
      .map {
        case (key, iter) => {
          /**
            * 通常iter自身的操作函数不足以满足需求时
            * 将iter转为 toList 获取更多的操作函数
            * 这里的分组后，会将同一个KEY的数据，放在一个新的分区中，后续处理时对应着一个TASK
            * 如果某个组的数据特别多时，就会出现数据倾斜，极可能出现OOM或很慢
            * 因此有必要分阶段聚合操作（分阶段排序）
            */
          val sortList = iter.toList.sorted.takeRight(3).reverse
          (key, sortList)
        }
      }

      /**
        * 第二阶段的分组与排序
        */
      // 去掉前缀，再此进行聚合操作
      // 这里的map出了两个连接的map(上方还有一个)
      // 可以优化到一个map中
      .map(tuple => {
      val key = tuple._1.split("_")(1)
      (key, tuple._2)
    })
      // 再次分组
      .groupByKey()
      .map {
        case (key, iter) => {
          val sortedList = iter.toList.flatMap(_.toList).sorted.takeRight(3).reverse
          (key, sortedList)
        }
      }

    println("-------------")
    sortTopRDD2.sortByKey().foreach(println)

    println("-------------")
    sortTopRDD2.coalesce(1).sortByKey().foreach(println)


    /**
      * 结果输出
      *
      * RDD#take 和 RDD#TOP 将数据以数据的形式返回给Driver,
      * 然后在Driver上显示
      *
      * 对RDD直接的操作，将在Executor中执行并显示。
      */


    /**
      * 关闭资源
      */
    // 解除缓存
    inputRDD.unpersist()

    // 为了开发测试时监控页面能够看到Job，线程暂停
    // Thread.sleep(10000 * 1000)
    sc.stop()
  }


  val text =
    """
      |aa 78
      |bb 69
      |cc 50
      |aa 68
      |bb 59
      |cc 40
      |aa 58
      |bb 49
      |cc 30
      |aa 48
      |bb 39
      |cc 20
      |aa 38
      |bb 29
      |cc 10
      |aa 78
      |bb 69
      |cc 50
      |aa 78
      |bb 69
      |cc 50
    """.stripMargin
}
