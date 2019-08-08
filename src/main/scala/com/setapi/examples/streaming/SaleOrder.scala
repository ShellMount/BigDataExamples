package com.setapi.examples.streaming

/**
  *
  * Created by ShellMount on 2019/7/31
  *
  **/

/**
  * 销售订单实体类：样例类
  * @param orderId
  * @param provinceId
  * @param orderPrice
  */
case class SaleOrder(
                    orderId: String,
                    provinceId: Int,
                    orderPrice: Float
                    )
