package com.setapi.flink_user_behavior_analysis.market_analysis

case class MarketingViewCount(
                               windowStart: String,
                               windowEnd: String,
                               channel: String,
                               behavior: String,
                               count: Long
                             )
