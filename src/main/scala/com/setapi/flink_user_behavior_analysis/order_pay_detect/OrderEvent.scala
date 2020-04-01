package com.setapi.flink_user_behavior_analysis.order_pay_detect

case class OrderEvent(
                     orderId: Long,
                     eventType: String,
                     txId: String,
                     eventTime: Long
                     )
