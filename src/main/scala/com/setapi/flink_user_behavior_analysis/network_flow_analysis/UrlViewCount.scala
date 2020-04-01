package com.setapi.flink_user_behavior_analysis.network_flow_analysis

case class UrlViewCount(
                       url: String,
                       windowEnd: Long,
                       count: Long
                       )
