package com.setapi.etlstreaming

/**
  *
  * Created by ShellMount on 2019/7/31
  *
  **/

/**
  * 销售订单实体类：样例类
  * @param orderType
  * @param orderId
  * @param provinceId
  * @param orderPrice
  */
case class EtlSaleOrder(
                         orderType: String,
                         orderId: String,
                         provinceId: Int,
                         orderPrice: Float
                       )
