package com.setapi.flink_user_behavior_analysis.market_analysis

case class AdBlackListWarning(
                             userId: Long,
                             adId: Long,
                             msg: String
                             )
