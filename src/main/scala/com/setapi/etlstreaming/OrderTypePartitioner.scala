package com.setapi.etlstreaming

import org.apache.spark.Partitioner

/**
  *
  * Created by ShellMount on 2019/8/8
  *
  * 自定义分区器，按照订单数据中订单支付的类型进行分区
  *
  *
  *
  **/

class OrderTypePartitioner extends Partitioner{
  // 支付订单类型 orderType 有四个值
  override def numPartitions: Int = 4

  // 定义依据订单支付类型得到该订单数据存储在RDD的哪个分区中，返回分区的编号
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[String] match {
      case "alipay" => 0
      case "weixin" => 1
      case "card" => 2
      case "other" => 3
    }
  }
}
