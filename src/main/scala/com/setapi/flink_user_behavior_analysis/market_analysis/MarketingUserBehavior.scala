package com.setapi.flink_user_behavior_analysis.market_analysis

case class MarketingUserBehavior(
                                userId: String,
                                behavior: String,
                                channel: String,
                                timestamp: Long
                                )
