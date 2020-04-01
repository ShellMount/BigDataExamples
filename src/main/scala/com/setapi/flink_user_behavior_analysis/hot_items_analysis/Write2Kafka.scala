package com.setapi.flink_user_behavior_analysis.hot_items_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object Write2Kafka {

  def main(args: Array[String]): Unit = {
    writeToKafka("HotItems")
  }


  def writeToKafka(str: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.213:9092")
    properties.setProperty("group.id", "flink-consumer-group")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // properties.setProperty("auto.offset.reset", "latest")

    // 定义一个KafkaProducer
    val producer = new KafkaProducer[String, String](properties)

    // 从文件中读取数据并发送
    val bufferSource = Source.fromFile("E:\\APP\\BigData\\api\\data\\flink\\UserBehavior.csv")

    for (line <- bufferSource.getLines()) {
      val record = new ProducerRecord[String, String]("HotItems", line)
      producer.send(record)
    }

    producer.close()

  }

}
