package com.setapi.etlstreaming

import java.util.{Properties, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Created by ShellMount on 2019/7/31
  *
  * 模拟产生订单数据 Order
  *
  **/

object EtlRealOrderJsonProducer {
  def main(args: Array[String]): Unit = {

    // JSON处理器
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // KAFKA Producer
    val props = new Properties()
    props.put("metadata.broker.list", EtlConstant.METADATA_BROKER_LIST)
    props.put("producer.type", EtlConstant.PRODUCER_TYPE)
    props.put("serializer.class", EtlConstant.SERIALIZER_CLASS)
    props.put("key.serializer.class", EtlConstant.SERIALIZER_CLASS)

    // 订单类型
    val orderTypeList = List("alipay", "weixin", "card", "other")


    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // 消息盒子
    val messageBox = new ArrayBuffer[KeyedMessage[String, String]]()

    try {
      while (true) {
        // 消息盒子
        messageBox.clear()
        val randomNum = EtlRandomUtil.getRandomNum(400) + 400
        for (index <- 0 until randomNum) {
          // TODO：构造数据 SaleOrder
          val orderItem = {
            val orderId = UUID.randomUUID()
            val provinceId = EtlRandomUtil.getRandomNum(34) + 1
            val orderPrice = EtlRandomUtil.getRandomNum(80) + 0.5f

            val orderType = orderTypeList(EtlRandomUtil.getRandomNum(4))
            EtlSaleOrder(orderType, orderId.toString, provinceId, orderPrice)
          }

          // TODO: 转换JSON格式
          val orderJson = mapper.writeValueAsString(orderItem)

          // TODO: JSON与类的互转
          println("--> " + orderJson)
          // println(s"Parse Json: ${mapper.readValue(orderJson, classOf[SaleOrder])}")

          // TODO: 发送数据到 TOPIC
          messageBox += new KeyedMessage(EtlConstant.TOPIC, orderItem.orderId, orderJson)
        }

        producer.send(messageBox: _*)
        Thread.sleep(EtlRandomUtil.getRandomNum(100) * 100)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != producer) producer.close()
    }
  }
}
