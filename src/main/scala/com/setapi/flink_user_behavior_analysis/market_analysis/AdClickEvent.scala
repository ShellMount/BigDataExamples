package com.setapi.flink_user_behavior_analysis.market_analysis

case class AdClickEvent (
                        userId: Long,
                        adId: Long,
                        province: String,
                        city: String,
                        timestamp: Long
                        )
