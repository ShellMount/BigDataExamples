package com.setapi.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * Created by ShellMount on 2019/8/1
  *
  * 需求：实时统计分析订单数据
  *   窗口统计TOP5订单量与累加和
  *
  *
  * 贷出函数：LoanFunction
  *   资源的管理，如创建关闭 StreamingContext
  * 用户函数：UserFunction
  *   用户业务实现的地方
  *
  *
  **/

object RealStreamingModuleSample {

  // 检查点
  val CHECK_POINT_PATH = "/tmp/spark/streaming/my_checkpoint/ckpt-201908011006"
  // BATCH INTER
  val STREAMING_BATCH_INTERVAL = Seconds(5)

  /**
    * 供出波函数：管理SC流式上下文对象的创建与关闭
    * @param args
    *            应用程序接收的参数
    * @param operation
    *                  用户函数：业务逻辑的处理
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
      // TODO: 设置StreamingContext 参数
      val ssc = new StreamingContext(sparkConf, STREAMING_BATCH_INTERVAL)
      // 设置日志级别
      ssc.sparkContext.setLogLevel("WARN")
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
      // 启动
      context.start()
      context.awaitTermination()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != context) context.stop(stopSparkContext = true, stopGracefully = true )
    }
  }

  /**
    * 用户函数：业务逻辑的处理
    *   实时获取KAFKA TOPIC中的数据
    * @param ssc
    *            流实例对象
    */
  def processStreamingData(ssc: StreamingContext): Unit = {
    // TODO: 从KAFKA读取数据

    // TODO: 分析处理

    // TODO: 输出数据

  }

  def main(args: Array[String]): Unit = {
    // 调用贷出函数，传递用户函数
    sparkOperation(args)(processStreamingData)
  }
}
