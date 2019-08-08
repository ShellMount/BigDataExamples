package com.setapi.examples.Utils

/**
  *
  * Created by ShellMount on 2019/7/31
  *
  **/

object Constant {
  // KAFKA CLUSTER BROKERS
  val METADATA_BROKER_LIST = "hdatanode2:9092"

  // OFFSET
  val AUTO_OFFSET_RESET = "largest"

  // 序列化类
  val SERIALIZER_CLASS = "kafka.serializer.StringEncoder"

  // 发送数据的方式
  val PRODUCER_TYPE = "async"

  // TOPIC
  val TOPIC = "MYFIRSTTOPIC"
}
