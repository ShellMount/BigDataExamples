package com.setapi.examples.streaming

import java.util.{Properties, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.setapi.examples.Utils.{Constant, RandomUtil}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Created by ShellMount on 2019/7/31
  *
  * 模拟产生订单数据 Order
  *
  **/

object RealOrderJsonProducer {
  def main(args: Array[String]): Unit = {

    // JSON处理器
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // KAFKA Producer
    val props = new Properties()
    props.put("metadata.broker.list", Constant.METADATA_BROKER_LIST)
    props.put("producer.type", Constant.PRODUCER_TYPE)
    props.put("serializer.class", Constant.SERIALIZER_CLASS)
    props.put("key.serializer.class", Constant.SERIALIZER_CLASS)

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // 消息盒子
    val messageBox = new ArrayBuffer[KeyedMessage[String, String]]()

    try {
      while (true) {
        // 消息盒子
        messageBox.clear()
        val randomNum = RandomUtil.getRandomNum(400) + 400
        for (index <- 0 until randomNum) {
          // TODO：构造数据 SaleOrder
          val orderItem = {
            val orderId = UUID.randomUUID()
            val provinceId = RandomUtil.getRandomNum(34) + 1
            val orderPrice = RandomUtil.getRandomNum(80) + 0.5f
            SaleOrder(orderId.toString, provinceId, orderPrice)
          }

          // TODO: 转换JSON格式
          val orderJson = mapper.writeValueAsString(orderItem)

          // TODO: JSON与类的互转
          println("--> " + orderJson)
          // println(s"Parse Json: ${mapper.readValue(orderJson, classOf[SaleOrder])}")

          // TODO: 发送数据到 TOPIC
          messageBox += new KeyedMessage(Constant.TOPIC, orderItem.orderId, orderJson)
        }

        producer.send(messageBox: _*)
        Thread.sleep(RandomUtil.getRandomNum(100) * 100)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != producer) producer.close()
    }
  }
}
