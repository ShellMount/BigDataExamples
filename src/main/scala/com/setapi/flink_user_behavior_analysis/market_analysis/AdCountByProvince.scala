package com.setapi.flink_user_behavior_analysis.market_analysis

case class AdCountByProvince(
                              windowEnd: Long,
                              province: String,
                              count: Long
                            )