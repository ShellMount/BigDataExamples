package com.setapi.flink_user_behavior_analysis.order_pay_detect

case class ReceiptEvent(
                       txId: String,
                       payChannel: String,
                       eventTime: Long
                       )
